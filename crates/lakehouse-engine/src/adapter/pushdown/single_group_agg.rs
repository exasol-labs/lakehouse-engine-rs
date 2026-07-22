//! Single-group aggregate detection and shared aggregate-item parsing.
//!
//! Extracted verbatim from the former flat `pushdown.rs`. `detect_aggregates`
//! is the single-group entry point; `parse_agg_item` is the shared parsing
//! primitive consumed by `grouped_agg` (hence `pub(super)`).

use crate::scan::spec::{AggKind, AggregatePlan};
use serde_json::Value as Json;
use vs_expression::render_expression;

/// One resolved single-group select-list item, in select-list order.
///
/// An ordinary aggregate ([`SingleGroupItem::Aggregate`]) is computed node-locally
/// as a partial result and merged by the outer wrapper's aggregate expression. A
/// `COUNT(DISTINCT ...)` ([`SingleGroupItem::Distinct`]) is NOT an aggregate
/// partial: it is decomposed into its own DISTINCT row-scan fan-out counted by an
/// outer Exasol-native `COUNT(DISTINCT "V")` — see
/// `vs-adapter/pushdown-planning-count-distinct`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingleGroupItem {
    /// An ordinary aggregate served by the shared per-shard partial-aggregate scan.
    Aggregate(AggregatePlan),
    /// A `COUNT(DISTINCT col|expr)` served by its own DISTINCT row-scan fan-out.
    Distinct(DistinctCount),
}

/// A single-group `COUNT(DISTINCT col)` / `COUNT(DISTINCT expr)` descriptor.
///
/// Exactly one of `column` (bare-column fast path) or `arg_expr` (a rendered
/// DataFusion SQL fragment) is populated, mirroring [`AggregatePlan`]. It carries
/// no `AggKind`: the distinct count is realized as a DISTINCT row-scan fan-out over
/// this argument (`CommonScanSpec::distinct`), not as an aggregate partial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctCount {
    pub column: Option<String>,
    pub arg_expr: Option<String>,
}

/// Inspect the pushdown request's `selectList` and return one resolved
/// [`SingleGroupItem`] per item, in order, if every select-list item is a
/// supported single-group aggregate.
///
/// Returns `None` (fall back to row scan) when any of the following hold:
/// - `groupBy` is present and non-empty (GROUP BY not supported)
/// - any select item has `distinct: true` OTHER than a `COUNT(DISTINCT ...)`
///   (single-group `COUNT(DISTINCT col)` / `COUNT(DISTINCT expr)` is accepted as a
///   [`SingleGroupItem::Distinct`] fan-out; DISTINCT SUM/AVG/etc. still decline)
/// - any select item is not one of COUNT(*), COUNT(col)/COUNT(expr),
///   SUM/MIN/MAX/AVG (bare column or renderable expression), or the
///   STDDEV/VARIANCE family
/// - the select list is absent or empty
pub fn detect_aggregates(pushdown_req: &Json) -> Option<Vec<SingleGroupItem>> {
    // Reject GROUP BY.
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;

    if list.is_empty() {
        return None;
    }

    let mut items = Vec::with_capacity(list.len());
    for item in list {
        // Every item must be a function_aggregate.
        if item.get("type").and_then(|t| t.as_str()) != Some("function_aggregate") {
            return None;
        }
        // A single-group COUNT(DISTINCT ...) becomes a DISTINCT row-scan fan-out;
        // every OTHER distinct aggregate declines via parse_agg_item.
        let resolved = match parse_count_distinct(item) {
            Some(distinct) => SingleGroupItem::Distinct(distinct),
            None => SingleGroupItem::Aggregate(parse_agg_item(item)?),
        };
        items.push(resolved);
    }

    Some(items)
}

/// The ordinary (non-distinct) aggregate plans among `items`, in order.
///
/// These drive the shared per-shard partial-aggregate scan and
/// [`validate_agg_col_types`](super::validate_agg_col_types); the distinct items
/// are handled separately as DISTINCT row-scan fan-outs.
///
/// `pub` (not `pub(super)`): this is the boundary that lets callers outside the
/// `pushdown` module (e.g. integration tests) obtain a nameable `Vec<AggregatePlan>`
/// from the opaque [`SingleGroupItem`] returned by [`detect_aggregates`] without
/// ever naming `SingleGroupItem` itself.
pub fn ordinary_plans(items: &[SingleGroupItem]) -> Vec<AggregatePlan> {
    items
        .iter()
        .filter_map(|item| match item {
            SingleGroupItem::Aggregate(plan) => Some(plan.clone()),
            SingleGroupItem::Distinct(_) => None,
        })
        .collect()
}

/// Whether any item is a `COUNT(DISTINCT ...)` needing its own row-scan fan-out.
pub(super) fn has_distinct(items: &[SingleGroupItem]) -> bool {
    items
        .iter()
        .any(|item| matches!(item, SingleGroupItem::Distinct(_)))
}

/// Whether the select list is EXACTLY one `COUNT(DISTINCT <bare column>)` and nothing
/// else — the ONLY shape that fans out to a dedicated DISTINCT row-scan counted by a
/// native `COUNT(DISTINCT "V")` (Case 1). The lone distinct must have a bare-column
/// argument (`dc.column.is_some()`): only a source column has a known exact Exasol
/// type to declare for the per-shard `"V"` value column, so cross-shard dedup stays
/// exact. A lone `COUNT(DISTINCT <expression>)` (`dc.column` is `None`, `dc.arg_expr`
/// set) is deliberately EXCLUDED — it would otherwise have to declare `"V"` as
/// `VARCHAR(2000000)` and rely on the expression's native→`Utf8` cast being injective,
/// which can silently undercount (e.g. two distinct timestamps that print identically
/// after string truncation). Any non-lone distinct shape (more than one distinct, or a
/// distinct alongside an ordinary SUM/MIN/MAX/COUNT/AVG aggregate — Case 2/3) is
/// likewise excluded. Every excluded shape declines the fan-out and routes to the
/// qualified single-table wrapper (`has_distinct && !is_lone_count_distinct` in
/// `mod.rs`), where Exasol evaluates any expression and the DISTINCT natively over
/// exact-typed base columns — and where no composition of several scalar-subquery
/// emitting-UDF calls in one select list would compile anyway (`sqlCode 04000`,
/// "emitting function in expression").
pub(super) fn is_lone_count_distinct(items: &[SingleGroupItem]) -> bool {
    matches!(items, [SingleGroupItem::Distinct(dc)] if dc.column.is_some())
}

/// Extract the column name (uppercase) from the first argument of an aggregate function.
fn column_from_first_arg(args: Option<&Vec<Json>>) -> Option<String> {
    args.and_then(|a| a.first()).and_then(|arg| {
        if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
            arg.get("name")
                .and_then(|n| n.as_str())
                .map(|s| s.to_uppercase())
        } else {
            None
        }
    })
}

/// Resolve an aggregate's single argument into either a bare-column name (the
/// fast path, populating `column`) or a rendered DataFusion SQL fragment
/// (populating `arg_expr`, via `vs_expression::render_expression` — the same
/// seam GROUP BY keys use).
///
/// Returns:
/// - `Some((Some(col), None))` when the argument is a bare `column` node — the
///   bare-column fast path, so the pre-existing exact-type MIN/MAX column
///   lookups keep working.
/// - `Some((None, Some(sql)))` when the argument is any other expression the VS
///   translator can render (e.g. `LENGTH(L_COMMENT)`).
/// - `None` when there is no argument, or the argument cannot be rendered — the
///   caller then declines the aggregate pushdown and falls back to row scanning.
fn arg_column_or_expr(args: Option<&Vec<Json>>) -> Option<(Option<String>, Option<String>)> {
    let arg = args.and_then(|a| a.first())?;
    if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
        return arg
            .get("name")
            .and_then(|n| n.as_str())
            .map(|s| (Some(s.to_uppercase()), None));
    }
    render_expression(arg).ok().map(|sql| (None, Some(sql)))
}

/// Parse a single-group `COUNT(DISTINCT ...)` select-list item into a
/// [`DistinctCount`] fan-out descriptor.
///
/// Handles both `COUNT(DISTINCT col)` (bare-column fast path) and
/// `COUNT(DISTINCT expr)` (rendered argument), mirroring how `COUNT(col)` /
/// `COUNT(expr)` are resolved. Returns `None` when the item is not a distinct
/// `COUNT`, or when its argument cannot be resolved to a column or rendered
/// expression — the single-group caller then defers to [`parse_agg_item`]
/// (which declines every other `distinct: true` item), so grouped
/// `COUNT(DISTINCT)` and other distinct aggregates still fall back to row scan.
fn parse_count_distinct(item: &Json) -> Option<DistinctCount> {
    if item.get("distinct").and_then(|d| d.as_bool()) != Some(true) {
        return None;
    }
    let fn_name = item
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_uppercase();
    if fn_name != "COUNT" {
        return None;
    }
    let args = item.get("arguments").and_then(|a| a.as_array());
    let (column, arg_expr) = arg_column_or_expr(args)?;
    Some(DistinctCount { column, arg_expr })
}

/// Parse a single `function_aggregate` select-list item into an `AggregatePlan`.
///
/// Returns `None` when the item uses `distinct: true` (single-group
/// `COUNT(DISTINCT)` is handled by [`parse_count_distinct`] before this is
/// called; every other distinct — and grouped `COUNT(DISTINCT)` — declines
/// here), when the function name is not one of COUNT, SUM, MIN, MAX, AVG, the
/// STDDEV/VARIANCE family, or when a COUNT/SUM/MIN/MAX/AVG argument is a scalar
/// expression the VS translator cannot render.
///
/// For COUNT/SUM/MIN/MAX/AVG a bare `column` argument takes the fast path
/// (`column` populated, `arg_expr` None); any other renderable expression is
/// carried in `arg_expr` (`column` None). The STDDEV/VARIANCE family keeps its
/// bare-column-only behavior unchanged.
///
/// The caller must verify `item.type == "function_aggregate"` before calling.
pub(super) fn parse_agg_item(item: &Json) -> Option<AggregatePlan> {
    if item.get("distinct").and_then(|d| d.as_bool()) == Some(true) {
        return None;
    }

    let fn_name = item
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_uppercase();

    let args = item.get("arguments").and_then(|a| a.as_array());

    let plan = match fn_name.as_str() {
        "COUNT" => match args.and_then(|a| a.first()) {
            // COUNT(*) — no argument: count every row.
            None => AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            // COUNT(col) fast path or COUNT(expr) rendered argument. An argument
            // that renders to neither a bare column nor a translatable expression
            // declines the whole aggregate pushdown (row-scan fallback).
            Some(_) => {
                let (column, arg_expr) = arg_column_or_expr(args)?;
                AggregatePlan {
                    kind: AggKind::CountCol,
                    column,
                    arg_expr,
                }
            }
        },
        "SUM" => {
            let (column, arg_expr) = arg_column_or_expr(args)?;
            AggregatePlan {
                kind: AggKind::Sum,
                column,
                arg_expr,
            }
        }
        "MIN" => {
            let (column, arg_expr) = arg_column_or_expr(args)?;
            AggregatePlan {
                kind: AggKind::Min,
                column,
                arg_expr,
            }
        }
        "MAX" => {
            let (column, arg_expr) = arg_column_or_expr(args)?;
            AggregatePlan {
                kind: AggKind::Max,
                column,
                arg_expr,
            }
        }
        "AVG" => {
            let (column, arg_expr) = arg_column_or_expr(args)?;
            AggregatePlan {
                kind: AggKind::Avg,
                column,
                arg_expr,
            }
        }
        // STDDEV/VARIANCE family — decompose into (cnt, sum, sum_sq) sufficient statistics.
        // STDDEV and STDDEV_SAMP are the sample forms; VARIANCE / VAR_SAMP likewise.
        "STDDEV" | "STDDEV_SAMP" => AggregatePlan {
            kind: AggKind::StddevSamp,
            column: column_from_first_arg(args),
            arg_expr: None,
        },
        "STDDEV_POP" => AggregatePlan {
            kind: AggKind::StddevPop,
            column: column_from_first_arg(args),
            arg_expr: None,
        },
        "VARIANCE" | "VAR_SAMP" => AggregatePlan {
            kind: AggKind::VarSamp,
            column: column_from_first_arg(args),
            arg_expr: None,
        },
        "VAR_POP" => AggregatePlan {
            kind: AggKind::VarPop,
            column: column_from_first_arg(args),
            arg_expr: None,
        },
        _ => return None,
    };
    Some(plan)
}

#[cfg(test)]
mod tests {
    use super::super::grouped_agg::{cast_merge_items, partial_emits_items};
    use super::super::test_support::*;
    use super::super::validate_agg_col_types;
    use super::*;

    /// Extract the [`AggregatePlan`] from an ordinary-aggregate item, panicking on a
    /// `COUNT(DISTINCT)` item — the detection tests assert the ordinary shape.
    fn agg_of(item: &SingleGroupItem) -> &AggregatePlan {
        match item {
            SingleGroupItem::Aggregate(plan) => plan,
            SingleGroupItem::Distinct(_) => panic!("expected an ordinary aggregate item"),
        }
    }

    /// Extract the [`DistinctCount`] from a `COUNT(DISTINCT)` item.
    fn distinct_of(item: &SingleGroupItem) -> &DistinctCount {
        match item {
            SingleGroupItem::Distinct(dc) => dc,
            SingleGroupItem::Aggregate(_) => panic!("expected a COUNT(DISTINCT) item"),
        }
    }

    /// `LENGTH(<col>)` scalar-expression node — renders to `character_length("<COL>")`.
    fn length_expr(col: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "LENGTH",
            "arguments": [{"type": "column", "name": col}],
        })
    }

    /// `<a> * <b>` two-column product node, as Exasol pushes it once `FN_MULT` is
    /// advertised (node name `MULT`; see decision-log entry [7]). Renders to
    /// `("<A>" * "<B>")` via the vs-expression translator.
    fn mult_expr(a: &str, b: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "MULT",
            "arguments": [
                {"type": "column", "name": a},
                {"type": "column", "name": b},
            ],
        })
    }

    /// COUNT(*) translates to Count with column=None.
    #[test]
    fn detect_count_star_produces_count_no_column() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", None, false)]
        });
        let plans = detect_aggregates(&req).expect("should detect COUNT(*)");
        assert_eq!(plans.len(), 1);
        assert_eq!(agg_of(&plans[0]).kind, AggKind::Count);
        assert!(agg_of(&plans[0]).column.is_none());
    }

    /// COUNT(col) translates to CountCol with the column name.
    #[test]
    fn detect_count_col_produces_count_col() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("amount"), false)]
        });
        let plans = detect_aggregates(&req).expect("should detect COUNT(col)");
        assert_eq!(agg_of(&plans[0]).kind, AggKind::CountCol);
        assert_eq!(agg_of(&plans[0]).column.as_deref(), Some("AMOUNT"));
    }

    /// SUM/MIN/MAX/AVG each translate to the right kind + column.
    #[test]
    fn detect_sum_min_max_avg_produce_correct_plans() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("SUM", Some("amount"), false),
                agg_item("MIN", Some("ts"), false),
                agg_item("MAX", Some("ts"), false),
                agg_item("AVG", Some("score"), false),
            ]
        });
        let plans = detect_aggregates(&req).expect("should detect all four");
        assert_eq!(agg_of(&plans[0]).kind, AggKind::Sum);
        assert_eq!(agg_of(&plans[0]).column.as_deref(), Some("AMOUNT"));
        assert_eq!(agg_of(&plans[1]).kind, AggKind::Min);
        assert_eq!(agg_of(&plans[1]).column.as_deref(), Some("TS"));
        assert_eq!(agg_of(&plans[2]).kind, AggKind::Max);
        assert_eq!(agg_of(&plans[2]).column.as_deref(), Some("TS"));
        assert_eq!(agg_of(&plans[3]).kind, AggKind::Avg);
        assert_eq!(agg_of(&plans[3]).column.as_deref(), Some("SCORE"));
    }

    /// GROUP BY present and non-empty => fall back (None).
    #[test]
    fn detect_aggregates_falls_back_on_group_by() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("amount"), false)],
            "groupBy": [{"type": "column", "name": "region"}],
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when GROUP BY is present"
        );
    }

    /// A non-COUNT DISTINCT aggregate (e.g. SUM DISTINCT) => fall back.
    /// (Single-group COUNT(DISTINCT) is now decomposed — see
    /// `count_distinct_builds_distinct_row_scan_spec`.)
    #[test]
    fn detect_aggregates_falls_back_on_distinct() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("amount"), true)]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when a non-COUNT DISTINCT is present"
        );
    }

    /// Unsupported aggregate function (e.g., MEDIAN) => fall back to row scan.
    /// Note: STDDEV is a supported decomposable aggregate via sufficient-statistics.
    #[test]
    fn detect_aggregates_falls_back_on_unsupported_function() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("SUM", Some("amount"), false),
                agg_item("MEDIAN", Some("amount"), false),
            ]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when any item is unsupported"
        );
    }

    /// Non-aggregate select item (e.g., plain column) => fall back.
    #[test]
    fn detect_aggregates_falls_back_on_column_select() {
        let req = serde_json::json!({
            "selectList": [
                {"type": "column", "name": "region"},
            ]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when select list contains non-aggregate"
        );
    }

    /// Empty select list => None.
    #[test]
    fn detect_aggregates_returns_none_for_empty_select_list() {
        let req = serde_json::json!({ "selectList": [] });
        assert!(detect_aggregates(&req).is_none());
    }

    /// Scenario (bare-column regression): COUNT(*), COUNT(col), SUM/MIN/MAX/AVG(col)
    /// and the STDDEV family keep the bare-column fast path — `column` populated,
    /// `arg_expr` None — so the pre-existing decomposition is byte-for-byte unchanged.
    #[test]
    fn bare_column_aggregates_unchanged_regression() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("COUNT", None, false),
                agg_item("COUNT", Some("id"), false),
                agg_item("SUM", Some("amount"), false),
                agg_item("MIN", Some("ts"), false),
                agg_item("MAX", Some("ts"), false),
                agg_item("AVG", Some("score"), false),
                agg_item("STDDEV", Some("score"), false),
            ]
        });
        let plans = detect_aggregates(&req).expect("bare-column aggregates must decompose");
        // Every plan takes the fast path: no rendered expression argument.
        assert!(
            plans.iter().all(|p| agg_of(p).arg_expr.is_none()),
            "bare-column aggregates must never populate arg_expr: {plans:?}"
        );
        assert_eq!(agg_of(&plans[0]).kind, AggKind::Count);
        assert!(agg_of(&plans[0]).column.is_none());
        assert_eq!(agg_of(&plans[1]).kind, AggKind::CountCol);
        assert_eq!(agg_of(&plans[1]).column.as_deref(), Some("ID"));
        assert_eq!(agg_of(&plans[2]).kind, AggKind::Sum);
        assert_eq!(agg_of(&plans[2]).column.as_deref(), Some("AMOUNT"));
        assert_eq!(agg_of(&plans[5]).kind, AggKind::Avg);
        assert_eq!(agg_of(&plans[6]).kind, AggKind::StddevSamp);

        // The partial EMITS clause is identical to the pre-change output: bare-column
        // SUM over DECIMAL widens to DECIMAL(36,s) from the COLUMN type (aggregate_types
        // is ignored for bare columns), independent of any declared aggregate type.
        let col_types = vec![
            ("AMOUNT".to_string(), "DECIMAL(20,0)".to_string()),
            ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
            ("TS".to_string(), "TIMESTAMP".to_string()),
        ];
        let sum_only = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }];
        // A misleading declared type must NOT override the bare-column source type.
        let emits = partial_emits_items(&sum_only, &col_types, &["VARCHAR(2000000)".to_string()]);
        assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,0)"#.to_string()]);
    }

    /// Scenario: a renderable scalar-expression argument is carried in `arg_expr`
    /// (not `column`), and the partial/merge column TYPE is derived from the
    /// aggregate item's declared type — SUM(expr)::DECIMAL widens to DECIMAL(36,s),
    /// SUM(expr)::DOUBLE stays DOUBLE, MIN/MAX(expr) take the declared type verbatim,
    /// and COUNT(expr) stays DECIMAL(20,0).
    #[test]
    fn expression_arg_partial_and_merge_types_from_declared_type() {
        // Detection: SUM(LENGTH(L_COMMENT)) renders the argument into arg_expr.
        let req = serde_json::json!({
            "selectList": [agg_item_expr("SUM", length_expr("L_COMMENT"), false)]
        });
        let plans = detect_aggregates(&req).expect("expression-argument SUM must decompose");
        assert_eq!(agg_of(&plans[0]).kind, AggKind::Sum);
        assert!(
            agg_of(&plans[0]).column.is_none(),
            "expression argument must not populate column"
        );
        assert_eq!(
            agg_of(&plans[0]).arg_expr.as_deref(),
            Some(r#"character_length("L_COMMENT")"#),
            "the rendered DataFusion fragment must be carried in arg_expr"
        );

        // Typing: no source column exists, so the type comes from the declared type.
        // There is deliberately NO matching entry in col_types.
        let col_types: Vec<(String, String)> = vec![];

        let sum_expr = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: None,
            arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
        }];
        // SUM(expr) declared DECIMAL(38,4) → partial widens to DECIMAL(36,4).
        let emits = partial_emits_items(&sum_expr, &col_types, &["DECIMAL(38,4)".to_string()]);
        assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()]);
        // SUM(expr) declared DOUBLE → partial stays DOUBLE PRECISION.
        let emits = partial_emits_items(&sum_expr, &col_types, &["DOUBLE PRECISION".to_string()]);
        assert_eq!(
            emits,
            vec![r#""PARTIAL_sum_0" DOUBLE PRECISION"#.to_string()]
        );

        // MIN(expr) takes the declared type verbatim.
        let min_expr = vec![AggregatePlan {
            kind: AggKind::Min,
            column: None,
            arg_expr: Some(r#"("A" + "B")"#.into()),
        }];
        let emits = partial_emits_items(&min_expr, &col_types, &["DATE".to_string()]);
        assert_eq!(emits, vec![r#""PARTIAL_min_0" DATE"#.to_string()]);

        // COUNT(expr) is a plain count → DECIMAL(20,0), declared type irrelevant.
        let count_expr = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: None,
            arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
        }];
        let emits = partial_emits_items(&count_expr, &col_types, &["DECIMAL(18,0)".to_string()]);
        assert_eq!(
            emits,
            vec![r#""PARTIAL_count_0" DECIMAL(20,0)"#.to_string()]
        );

        // An expression SUM/MIN/MAX validates (its declared type is numeric in
        // practice; the missing column resolves to the numeric DOUBLE fallback).
        assert!(
            validate_agg_col_types(&sum_expr, &col_types),
            "expression-argument SUM must pass validation, not force a row scan"
        );
    }

    /// Scenario (NQ1 / TPC-H Q6 shape): `SUM(L_EXTENDEDPRICE * L_DISCOUNT)` over two
    /// DECIMAL(15,2) columns. Exasol declares the SUM result as DECIMAL(36,4) (it
    /// widens the DECIMAL(30,4) product's precision to its max 36, holding the
    /// natural scale 4 — verified live, decision-log entry [7]). The partial column
    /// must be sized from that declared type — NOT recomputed from the operands'
    /// own DECIMAL(15,2) types — so it widens to DECIMAL(36,4), and the merge casts
    /// to the same declared DECIMAL(36,4). This exercises the DECIMAL-with-nonzero-
    /// scale declared-type path for a two-column product argument.
    #[test]
    fn decimal_product_sum_partial_widens_to_decimal_36() {
        // Detection: SUM(L_EXTENDEDPRICE * L_DISCOUNT) carries the product in
        // arg_expr (no bare source column) — proving the aggregate is decomposed,
        // not declined into a raw two-column row scan.
        let req = serde_json::json!({
            "selectList": [
                agg_item_expr("SUM", mult_expr("L_EXTENDEDPRICE", "L_DISCOUNT"), false)
            ]
        });
        let items =
            detect_aggregates(&req).expect("SUM(col * col) must decompose, not fall back to scan");
        assert_eq!(items.len(), 1);
        let plans = ordinary_plans(&items);
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert!(
            plans[0].column.is_none(),
            "a two-column product has no single source column"
        );
        assert_eq!(
            plans[0].arg_expr.as_deref(),
            Some(r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#),
            "the rendered product must be carried in arg_expr"
        );

        // Typing is driven purely by Exasol's declared result type; there is
        // deliberately NO operand column in col_types (the product has none), so a
        // type recomputed from operands would have to reimplement Exasol's widening
        // rules. The declared DECIMAL(36,4) is authoritative and read verbatim.
        let col_types: Vec<(String, String)> = vec![];
        let declared = ["DECIMAL(36,4)".to_string()];

        let emits = partial_emits_items(&plans, &col_types, &declared);
        assert_eq!(
            emits,
            vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()],
            "partial SUM column must widen to the declared DECIMAL(36,4)"
        );

        // The merge wrapper casts the summed partial back to the declared type so
        // it matches Exasol's positional selectListDataTypes validation.
        let merge = cast_merge_items(&plans, &declared);
        assert_eq!(
            merge,
            vec![r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,4))"#.to_string()],
            "merge must cast the summed partial to the declared DECIMAL(36,4)"
        );

        // The expression-argument SUM validates (no operand column → numeric
        // DOUBLE fallback), so it is never forced into a row scan.
        assert!(validate_agg_col_types(&plans, &col_types));
    }

    /// Scenario: an aggregate whose argument the VS translator cannot render
    /// declines the whole aggregate pushdown (row-scan fallback), rather than
    /// emitting a plan referencing an argument it could not render soundly.
    #[test]
    fn unrenderable_agg_arg_falls_back_to_row_scan() {
        let unknown = serde_json::json!({
            "type": "function_scalar",
            "name": "TOTALLY_UNKNOWN_FN",
            "arguments": [{"type": "column", "name": "id"}],
        });
        for name in &["SUM", "MIN", "MAX", "AVG", "COUNT"] {
            let req = serde_json::json!({
                "selectList": [agg_item_expr(name, unknown.clone(), false)]
            });
            assert!(
                detect_aggregates(&req).is_none(),
                "{name} over an unrenderable argument must fall back to row scan"
            );
        }
        // A distinct COUNT over an unrenderable argument also falls back.
        let req = serde_json::json!({
            "selectList": [agg_item_expr("COUNT", unknown.clone(), true)]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "COUNT(DISTINCT unrenderable) must fall back to row scan"
        );
    }

    /// Scenario: single-group COUNT(DISTINCT col) is decomposed into a DISTINCT
    /// row-scan fan-out descriptor ([`SingleGroupItem::Distinct`], bare column
    /// populated); COUNT(DISTINCT expr) carries the rendered argument; and neither
    /// contributes an ordinary aggregate plan (so no partial-aggregate column is
    /// emitted for it — the count is a native `COUNT(DISTINCT "V")` over the fan-out).
    #[test]
    fn count_distinct_builds_distinct_row_scan_spec() {
        // COUNT(DISTINCT L_SHIPMODE) — bare column fast path.
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("L_SHIPMODE"), true)]
        });
        let items = detect_aggregates(&req).expect("single-group COUNT(DISTINCT) must decompose");
        assert_eq!(items.len(), 1);
        assert!(has_distinct(&items), "the item must be a distinct fan-out");
        let dc = distinct_of(&items[0]);
        assert_eq!(dc.column.as_deref(), Some("L_SHIPMODE"));
        assert!(dc.arg_expr.is_none());
        // A distinct item is NOT an ordinary aggregate: it drives no partial column.
        assert!(
            ordinary_plans(&items).is_empty(),
            "a COUNT(DISTINCT) item must not appear among the ordinary aggregate plans"
        );

        // COUNT(DISTINCT LENGTH(col)) — rendered expression argument.
        let req_expr = serde_json::json!({
            "selectList": [agg_item_expr("COUNT", length_expr("L_COMMENT"), true)]
        });
        let items_expr = detect_aggregates(&req_expr).expect("COUNT(DISTINCT expr) must decompose");
        let dc_expr = distinct_of(&items_expr[0]);
        assert!(dc_expr.column.is_none());
        assert_eq!(
            dc_expr.arg_expr.as_deref(),
            Some(r#"character_length("L_COMMENT")"#)
        );
    }

    /// Scenario (task 6.5): single-group `COUNT(DISTINCT)` detection fans out ONLY a
    /// lone distinct (Case 1 — `is_lone_count_distinct` true), and declines every
    /// multi-distinct or distinct-plus-ordinary-aggregate shape (Case 2/3 —
    /// `is_lone_count_distinct` false while `has_distinct` stays true), which is the
    /// dispatch condition the `mod.rs` Case 2/3 guard uses to route the request to the
    /// qualified single-table wrapper instead of the fan-out.
    #[test]
    fn multi_count_distinct_declines_to_qualified_wrapper() {
        // Case 1: exactly one COUNT(DISTINCT), nothing else → fans out.
        let lone = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("CATEGORY"), true)],
        });
        let items = detect_aggregates(&lone).expect("a lone COUNT(DISTINCT) decomposes");
        assert!(
            has_distinct(&items) && is_lone_count_distinct(&items),
            "a lone COUNT(DISTINCT) is the only shape that fans out (Case 1)"
        );

        // Case 2: more than one COUNT(DISTINCT) → declines the fan-out.
        let multi = serde_json::json!({
            "selectList": [
                agg_item("COUNT", Some("CATEGORY"), true),
                agg_item("COUNT", Some("REGION"), true),
            ],
        });
        let items = detect_aggregates(&multi).expect("multiple distinct items still detect");
        assert!(
            has_distinct(&items) && !is_lone_count_distinct(&items),
            "more than one COUNT(DISTINCT) must decline the fan-out (Case 2)"
        );

        // Case 3: a COUNT(DISTINCT) mixed with an ordinary aggregate → declines.
        let mixed = serde_json::json!({
            "selectList": [
                agg_item("COUNT", Some("CATEGORY"), true),
                agg_item("SUM", Some("AMOUNT"), false),
            ],
        });
        let items = detect_aggregates(&mixed).expect("distinct-plus-ordinary still detects");
        assert!(
            has_distinct(&items) && !is_lone_count_distinct(&items),
            "a distinct mixed with an ordinary aggregate must decline the fan-out (Case 3)"
        );
    }

    /// Scenario (PR #163 review finding, task 1.4): a LONE
    /// `COUNT(DISTINCT <expression>)` (e.g. `COUNT(DISTINCT LENGTH(name))`, nothing
    /// else in the select list) must NOT take the per-shard fan-out. After narrowing
    /// `is_lone_count_distinct` to bare-column arguments, an expression argument makes
    /// `is_lone_count_distinct` false while `has_distinct` stays true — which is the
    /// EXACT dispatch condition (`has_distinct && !is_lone_count_distinct`) the `mod.rs`
    /// Case 2/3 guard uses to decline the fan-out and route to the qualified
    /// single-table wrapper, where Exasol evaluates the expression and DISTINCT natively
    /// over exact-typed base columns (no `arrow::compute::cast(.., Utf8)` injectivity
    /// dependency, which could silently undercount). A lone BARE-COLUMN distinct is
    /// unaffected — it still fans out (Case 1).
    ///
    /// This is the direct regression test for the dispatch narrowing: before it, a lone
    /// expression distinct wrongly matched "lone" and fanned out with a VARCHAR-typed
    /// `"V"`; it must now route exactly like a genuine multi-distinct/mixed Case 2/3.
    #[test]
    fn lone_expression_count_distinct_declines_fan_out_to_wrapper() {
        // Lone COUNT(DISTINCT LENGTH(NAME)) — a single expression-argument distinct.
        let expr = serde_json::json!({
            "selectList": [agg_item_expr("COUNT", length_expr("NAME"), true)],
        });
        let items = detect_aggregates(&expr)
            .expect("a lone COUNT(DISTINCT expr) still decomposes to a distinct item");
        assert_eq!(items.len(), 1);
        let dc = distinct_of(&items[0]);
        assert!(
            dc.column.is_none() && dc.arg_expr.is_some(),
            "the argument is an expression, so the distinct carries a rendered arg_expr, \
             not a bare column"
        );
        assert!(
            has_distinct(&items),
            "a lone expression COUNT(DISTINCT) is still a distinct request"
        );
        assert!(
            !is_lone_count_distinct(&items),
            "an EXPRESSION-argument lone COUNT(DISTINCT) must NOT fan out — it declines to \
             the qualified single-table wrapper exactly like a Case 2/3 request (this is \
             the regression: before the dispatch narrowing it wrongly fanned out with a \
             VARCHAR-typed \"V\")"
        );

        // Contrast: a lone BARE-COLUMN COUNT(DISTINCT) IS a lone distinct — still fans out.
        let bare = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("NAME"), true)],
        });
        let bare_items =
            detect_aggregates(&bare).expect("a lone bare-column COUNT(DISTINCT) decomposes");
        assert!(
            is_lone_count_distinct(&bare_items),
            "a lone BARE-COLUMN COUNT(DISTINCT) is unaffected — it still fans out (Case 1)"
        );
    }

    /// MEDIAN, *_DISTINCT, APPROX_COUNT_DISTINCT, LISTAGG, GROUP_CONCAT all cause
    /// parse_agg_item / detect_aggregates to return None (row-scan fallback).
    #[test]
    fn non_decomposable_aggregate_falls_back_to_row_scan() {
        for name in &[
            "MEDIAN",
            "APPROXIMATE_COUNT_DISTINCT",
            "LISTAGG",
            "GROUP_CONCAT",
        ] {
            let req = serde_json::json!({
                "selectList": [agg_item(name, Some("AMOUNT"), false)],
            });
            assert!(
                detect_aggregates(&req).is_none(),
                "{name} must fall back to row scan"
            );
        }
        // A non-COUNT DISTINCT (SUM DISTINCT) is not decomposable — falls back.
        // (Single-group COUNT(DISTINCT) IS decomposed; see
        // `count_distinct_builds_distinct_row_scan_spec`.)
        let req_distinct = serde_json::json!({
            "selectList": [agg_item("SUM", Some("AMOUNT"), true)],
        });
        assert!(
            detect_aggregates(&req_distinct).is_none(),
            "SUM(DISTINCT) must fall back to row scan"
        );
    }

    /// parse_agg_item returns a stat plan for STDDEV/VARIANCE family names.
    #[test]
    fn parse_agg_item_recognises_stat_functions() {
        for (name, expected_kind) in &[
            ("STDDEV", AggKind::StddevSamp),
            ("STDDEV_SAMP", AggKind::StddevSamp),
            ("STDDEV_POP", AggKind::StddevPop),
            ("VARIANCE", AggKind::VarSamp),
            ("VAR_SAMP", AggKind::VarSamp),
            ("VAR_POP", AggKind::VarPop),
        ] {
            let item = agg_item(name, Some("AMOUNT"), false);
            let plan =
                parse_agg_item(&item).unwrap_or_else(|| panic!("{name} must parse to a stat plan"));
            assert_eq!(
                plan.kind, *expected_kind,
                "{name} must map to {:?}",
                expected_kind
            );
            assert_eq!(plan.column.as_deref(), Some("AMOUNT"));
        }
    }
}
