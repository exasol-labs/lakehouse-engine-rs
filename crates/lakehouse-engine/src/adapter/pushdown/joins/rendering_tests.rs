use super::super::super::support::collect_all_column_names;
use super::super::planning::{JoinSides, disjoint_schema_guard};
use super::super::sql_builders::{
    JoinScanTuning, RenderedJoinPushdown, build_broadcast_join_sql, build_n_scan_join_sql,
    build_side_fan_out_sql, render_broadcast_join,
};
use super::super::tests::{
    detected_join, equi_condition, join_request, resolved_side, two_scan_tuning,
};
use super::*;
use crate::adapter::pushdown::support::apply_type_rewrites;
use crate::adapter::pushdown::test_support::*;
use vs_expression::{render_df_filter_safe, render_expression_safe};

// ---------------------------------------------------------------------------
// Join rendering: disjoint-column guard + condition/filter/projection
// rendering via the reused vs-expression translator.
// ---------------------------------------------------------------------------

/// Two tables whose column names are genuinely disjoint (TPC-H `C_*` vs `O_*`)
/// pass the guard, so bare column names resolve unambiguously.
#[test]
fn disjoint_schema_guard_passes_for_disjoint_column_names() {
    let request = join_request(Json::Null, equi_condition());
    let left = involved_table_columns(&request, "CUSTOMER");
    let right = involved_table_columns(&request, "ORDERS");
    assert!(
        disjoint_schema_guard(&left, &right),
        "C_* and O_* column sets are disjoint and must pass the guard"
    );
}

/// ANY overlapping column name fails the guard, and the failure is surfaced as
/// a clean decline (`Ok(None)`) — the caller falls through to the unaccelerated
/// path — never as an error.
#[test]
fn overlapping_column_name_fails_guard_and_declines_without_error() {
    let mut request = join_request(Json::Null, equi_condition());
    // Give BOTH sides a column with the same name.
    for table_idx in [0, 1] {
        request["involvedTables"][table_idx]["columns"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "name": "SHARED_KEY",
                "dataType": {"type": "varchar", "size": 10}
            }));
    }

    let left = involved_table_columns(&request, "CUSTOMER");
    let right = involved_table_columns(&request, "ORDERS");
    assert!(
        !disjoint_schema_guard(&left, &right),
        "a shared column name must fail the disjoint guard"
    );

    // The whole rendering entry point declines cleanly, not with an Err.
    let detected = detected_join(&request);
    let outcome = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("a guard failure is a decline, not an error");
    assert!(
        outcome.is_none(),
        "a column-name collision must decline to the unaccelerated path"
    );
}

/// A simple equi-condition renders to the correct DataFusion SQL boolean
/// expression via the reused translator, and is threaded verbatim into the
/// rendered join's `condition` (→ `JoinSpec::condition`).
#[test]
fn join_condition_renders_via_translator() {
    assert_eq!(
        render_expression_safe(&equi_condition()).as_deref(),
        Some(r#"("C_CUSTKEY" = "O_CUSTKEY")"#),
        "the equi-condition must render to a bare-name DataFusion boolean expr"
    );

    let request = join_request(Json::Null, equi_condition());
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("disjoint, renderable join")
        .expect("a disjoint join must render, not decline");
    assert_eq!(rendered.condition, r#"("C_CUSTKEY" = "O_CUSTKEY")"#);
}

/// A WHERE filter referencing columns from BOTH sides renders correctly against
/// the combined schema (bare names, disjoint → unambiguous).
#[test]
fn join_where_filter_spanning_both_sides_renders() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
             "right": {"type": "literal_string", "value": "ACME"}},
            {"type": "predicate_greater",
             "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
             "right": {"type": "literal_string", "value": "1995-01-01"}},
        ],
    });

    let detected = detected_join(&request);
    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("disjoint, renderable join")
        .expect("must render");
    let filter = rendered
        .filter
        .expect("a cross-side WHERE filter must render");
    assert!(
        filter.contains(r#""C_NAME""#),
        "the left-side column must appear in the rendered filter: {filter}"
    );
    assert!(
        filter.contains(r#""O_ORDERDATE""#),
        "the right-side column must appear in the rendered filter: {filter}"
    );
    assert!(
        filter.contains("AND"),
        "the conjunction of both sides must render: {filter}"
    );
}

/// The cross-table projection attributes each projected column to its OWNING
/// side's Exasol type: `C_NAME` from CUSTOMER (`VARCHAR(100)`), `O_ORDERDATE`
/// from ORDERS (`DATE`).
#[test]
fn join_projection_emits_attribute_each_side_owning_type() {
    let request = join_request(Json::Null, equi_condition());
    let detected = detected_join(&request);
    let (projection, types, _widened) =
        extract_join_projection(&request, &pd(&request), &detected).expect("projectable");

    assert_eq!(
        projection,
        vec![
            ProjectionItem::Column("C_NAME".into()),
            ProjectionItem::Column("O_ORDERDATE".into()),
        ],
        "projection spans both tables in select-list order"
    );
    assert_eq!(
        types,
        vec!["VARCHAR(100)".to_string(), "DATE".to_string()],
        "each column's EMITS type comes from the side that owns it"
    );
}

/// A `function_scalar_cast` over a side column in a join's select list
/// resolves through `extract_join_projection` to a `ProjectionItem::Expr`,
/// NOT the two-table full-row fallback (issue #136). `extract_join_projection`
/// reuses `project_columns` verbatim against the disjoint union of both
/// tables' columns, so the same dispatch fix that covers the single-table
/// row-scan path (`support.rs`) must also cover this join path.
#[test]
fn join_projection_resolves_cast_node_to_expr_not_full_row_fallback() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["selectList"] = serde_json::json!([
        {
            "type": "function_scalar_cast",
            "name": "CAST",
            "dataType": {"type": "varchar", "size": 2000000},
            "arguments": [{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}]
        }
    ]);

    let detected = detected_join(&request);
    let (projection, _types, _widened) =
        extract_join_projection(&request, &pd(&request), &detected).expect("projectable");

    assert_eq!(
        projection.len(),
        1,
        "a function_scalar_cast select-list item must not fall back to the two-table \
         full base row: {projection:?}"
    );
    assert!(
        matches!(projection[0], ProjectionItem::Expr { .. }),
        "a rendered CAST expression must be an Expr projection item, not a bare Column: \
         {projection:?}"
    );
}

/// `string_function_arg_type_guard` (issue #210) reaches through the join-shared
/// `project_columns`, exercised across two calls into `extract_join_projection`
/// on the same detected join:
///
/// (a) `UPPER(C_CUSTKEY)` (CUSTOMER's DECIMAL column) still projects as a single
///     coerced `ProjectionItem::Expr` carrying the trimmed decimal-to-string
///     form — proving coercion reaches through the join-shared `project_columns`,
///     not just the single-table path.
/// (b) A decline falls back to the FULL projection over the UNION of BOTH
///     joined tables' columns, not just one side.
///
/// `join_request`'s fixture carries no DOUBLE-typed column on either side, so the
/// decline trigger used for (b) is the #228 ARITY decline instead —
/// `INSTR(C_NAME, 'b', 3)`, three arguments, over CUSTOMER's own VARCHAR column —
/// which reaches the exact same `None` path a type decline would, with no
/// fixture change.
#[test]
fn join_projection_string_fn_coerces_decimal_and_declines_unrenderable_arity() {
    let request = join_request(Json::Null, equi_condition());
    let detected = detected_join(&request);

    let mut coerce_request = request.clone();
    coerce_request["pushdownRequest"]["selectList"] = serde_json::json!([
        {
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [{"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"}]
        }
    ]);
    let (projection, _types, _widened) =
        extract_join_projection(&coerce_request, &pd(&coerce_request), &detected)
            .expect("projectable");
    assert_eq!(
        projection.len(),
        1,
        "UPPER(C_CUSTKEY) must project a single expression, not the full two-table \
         row: {projection:?}"
    );
    let ProjectionItem::Expr { expr } = &projection[0] else {
        panic!("must be a rendered expression, not a bare column: {projection:?}");
    };
    assert!(
        expr.contains(r#"upper(regexp_replace(regexp_replace(CAST("C_CUSTKEY" AS VARCHAR)"#),
        "UPPER's DECIMAL argument must render through the trimmed decimal-to-string \
         form: {expr}"
    );

    let mut decline_request = request.clone();
    decline_request["pushdownRequest"]["selectList"] = serde_json::json!([
        {
            "type": "function_scalar",
            "name": "INSTR",
            "arguments": [
                {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                {"type": "literal_string", "value": "b"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        }
    ]);
    let (projection, _types, _widened) =
        extract_join_projection(&decline_request, &pd(&decline_request), &detected)
            .expect("projectable");
    let expected_full_row_len = involved_table_columns(&decline_request, "CUSTOMER").len()
        + involved_table_columns(&decline_request, "ORDERS").len();
    assert_eq!(
        projection.len(),
        expected_full_row_len,
        "the arity-decline INSTR must fall back to the full projection over BOTH \
         joined tables' columns, not a truncated strpos: {projection:?}"
    );
}

/// `like_subject_type_guard` (issue #219) reaches through the join-shared
/// `project_columns`, exercised across two calls into `extract_join_projection`
/// on the same detected join — the select-list analog of
/// [`join_projection_string_fn_coerces_decimal_and_declines_unrenderable_arity`]:
///
/// (a) `C_NAME LIKE 'A%'` (CUSTOMER's VARCHAR(100) column) still projects as a
///     single `ProjectionItem::Expr`, proving the guard's pass-through for a
///     string subject reaches the broadcast-join SELECT list.
/// (b) `C_CUSTKEY LIKE '1%'` (CUSTOMER's DECIMAL column) declines and falls back
///     to the FULL projection over the UNION of BOTH joined tables' columns —
///     the reach this plan wires by adding `like_subject_type_guard` as the
///     first pass of `apply_type_rewrites`.
#[test]
fn join_projection_like_guard_reaches_join_select_list() {
    let request = join_request(Json::Null, equi_condition());
    let detected = detected_join(&request);

    let mut string_request = request.clone();
    string_request["pushdownRequest"]["selectList"] = serde_json::json!([
        {
            "type": "predicate_like",
            "expression": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
            "pattern": {"type": "literal_string", "value": "A%"}
        }
    ]);
    let (projection, _types, widened) =
        extract_join_projection(&string_request, &pd(&string_request), &detected)
            .expect("projectable");
    assert!(
        !widened,
        "a VARCHAR subject must keep the broadcast projection, not widen to the \
         full row: {projection:?}"
    );
    assert_eq!(
        projection.len(),
        1,
        "C_NAME LIKE 'A%' must project a single expression, not the full two-table \
         row: {projection:?}"
    );
    let ProjectionItem::Expr { expr } = &projection[0] else {
        panic!("must be a rendered expression, not a bare column: {projection:?}");
    };
    assert!(
        expr.contains("C_NAME") && expr.contains("LIKE"),
        "the VARCHAR subject must render as a LIKE expression over C_NAME: {expr}"
    );

    let mut decline_request = request.clone();
    decline_request["pushdownRequest"]["selectList"] = serde_json::json!([
        {
            "type": "predicate_like",
            "expression": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "pattern": {"type": "literal_string", "value": "1%"}
        }
    ]);
    let (projection, _types, widened) =
        extract_join_projection(&decline_request, &pd(&decline_request), &detected)
            .expect("projectable");
    assert!(
        widened,
        "the widening flag is what declines the broadcast join to the N-scan \
         fallback (joins/sql_builders.rs:85); a DECIMAL-subject LIKE must set it: \
         {projection:?}"
    );
    let expected_full_row_len = involved_table_columns(&decline_request, "CUSTOMER").len()
        + involved_table_columns(&decline_request, "ORDERS").len();
    assert_eq!(
        projection.len(),
        expected_full_row_len,
        "a DECIMAL-subject LIKE must fall back to the full projection over BOTH \
         joined tables' columns, not an unguarded LIKE: {projection:?}"
    );
    assert!(
        projection
            .iter()
            .all(|item| matches!(item, ProjectionItem::Column(_))),
        "the fallback projection must be bare columns, not a same-length vector \
         of rendered Expr items: {projection:?}"
    );
}

// -----------------------------------------------------------------------
// Per-side pruning: side-local conjunct attribution, projection narrowing,
// and per-side filter pushdown in the fallback path.
// -----------------------------------------------------------------------

/// A conjunct referencing only one side's columns is attributed to that side
/// alone: the CUSTOMER-only conjunct threads to CUSTOMER, the ORDERS-only
/// conjunct to ORDERS, and neither leaks to the other.
#[test]
fn side_local_filter_attributes_conjuncts_to_owning_side() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
             "right": {"type": "literal_string", "value": "ACME"}},
            {"type": "predicate_greater",
             "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
             "right": {"type": "literal_string", "value": "1995-01-01"}},
        ],
    });

    let cust = render_df_filter_safe(
        &side_local_filter(&filter, "CUSTOMER").expect("a CUSTOMER-local conjunct exists"),
    )
    .expect("renders");
    assert!(
        cust.contains("C_NAME") && !cust.contains("O_ORDERDATE"),
        "CUSTOMER side-local filter must carry only C_NAME: {cust}"
    );

    let ord = render_df_filter_safe(
        &side_local_filter(&filter, "ORDERS").expect("an ORDERS-local conjunct exists"),
    )
    .expect("renders");
    assert!(
        ord.contains("O_ORDERDATE") && !ord.contains("C_NAME"),
        "ORDERS side-local filter must carry only O_ORDERDATE: {ord}"
    );
}

/// A cross-table conjunct (references both sides) and an OR spanning both sides
/// are withheld from BOTH sides' pruning — only the outer wrapper's WHERE
/// applies them. A single-side-local conjunct alongside a cross-table one is
/// still extracted for its side.
#[test]
fn side_local_filter_withholds_cross_table_and_or_conjuncts() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            // cross-table: references CUSTOMER and ORDERS
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
             "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}},
            // CUSTOMER-local
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
             "right": {"type": "literal_string", "value": "ACME"}},
        ],
    });
    let cust = render_df_filter_safe(
        &side_local_filter(&filter, "CUSTOMER").expect("CUSTOMER-local conjunct present"),
    )
    .expect("renders");
    assert!(
        cust.contains("C_NAME") && !cust.contains("O_CUSTKEY"),
        "the cross-table conjunct must NOT be pushed to CUSTOMER: {cust}"
    );
    assert!(
        side_local_filter(&filter, "ORDERS").is_none(),
        "ORDERS is only referenced by the cross-table conjunct, so nothing is side-local to it"
    );

    // An OR spanning both sides is one opaque conjunct referencing both → withheld.
    let or_filter = serde_json::json!({
        "type": "predicate_or",
        "expressions": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
             "right": {"type": "literal_string", "value": "ACME"}},
            {"type": "predicate_greater",
             "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
             "right": {"type": "literal_string", "value": "1995-01-01"}},
        ],
    });
    assert!(side_local_filter(&or_filter, "CUSTOMER").is_none());
    assert!(side_local_filter(&or_filter, "ORDERS").is_none());

    // An OR referencing only ONE side is side-local to it (still prunable).
    let one_side_or = serde_json::json!({
        "type": "predicate_or",
        "expressions": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
             "right": {"type": "literal_string", "value": "ACME"}},
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
             "right": {"type": "literal_string", "value": "GLOBEX"}},
        ],
    });
    assert!(
        side_local_filter(&one_side_or, "CUSTOMER").is_some(),
        "an OR over one side alone is side-local and prunable"
    );
    assert!(side_local_filter(&one_side_or, "ORDERS").is_none());
}

/// A filter that is a single (non-AND) conjunct is attributed to its owning side
/// without a top-level AND wrapper.
#[test]
fn side_local_filter_handles_a_single_conjunct() {
    let single = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
        "right": {"type": "literal_string", "value": "ACME"}
    });
    assert!(side_local_filter(&single, "CUSTOMER").is_some());
    assert!(side_local_filter(&single, "ORDERS").is_none());
}

/// Attribution is by `tableName`, NOT by column name: with a column name shared
/// across both tables (`ID`), a conjunct on `EVENTS.ID` is side-local to EVENTS
/// only and is never applied to LABELS (which also has an `ID`). This is the
/// shared-column-name safety the whole per-side pruning rests on.
#[test]
fn side_local_filter_attributes_shared_column_by_table_not_name() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_greater",
             "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
             "right": {"type": "literal_exactnumeric", "value": 5}},
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "LABEL", "tableName": "LABELS"},
             "right": {"type": "literal_string", "value": "x"}},
        ],
    });

    let events = render_df_filter_safe(
        &side_local_filter(&filter, "EVENTS").expect("EVENTS.ID conjunct is side-local"),
    )
    .expect("renders");
    assert!(
        events.contains("ID") && events.contains('5'),
        "EVENTS side-local filter must carry the ID > 5 predicate: {events}"
    );

    let labels = render_df_filter_safe(
        &side_local_filter(&filter, "LABELS").expect("LABELS.LABEL conjunct is side-local"),
    )
    .expect("renders");
    assert!(
        labels.contains("LABEL") && !labels.contains('5'),
        "the EVENTS.ID predicate must NOT be applied to LABELS despite the shared name: {labels}"
    );
}

/// The ORDERS-side-local conjunct the DataFusion dialect CAN express.
fn orders_local_rendering_conjunct() -> Json {
    serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
        "right": {"type": "literal_string", "value": "1995-01-01"}
    })
}

/// The ORDERS-side-local conjunct the DataFusion dialect REFUSES (its `SECOND`
/// field shortcut permits exactly one argument) while Exasol renders it — the
/// dialect asymmetry the render-site screen exists to route.
fn orders_local_declined_conjunct() -> Json {
    serde_json::json!({
        "type": "predicate_greater",
        "left": {
            "type": "function_scalar",
            "name": "SECOND",
            "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                {"type": "literal_exactnumeric", "value": 3}
            ]
        },
        "right": {"type": "literal_exactnumeric", "value": 1}
    })
}

/// Both ORDERS-side-local conjuncts under one AND: one renders for DataFusion,
/// one declines.
fn orders_local_rendering_and_declined_filter() -> Json {
    serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            orders_local_rendering_conjunct(),
            orders_local_declined_conjunct(),
        ],
    })
}

/// A side-local conjunct whose DataFusion render DECLINES is reclassified as
/// residual: `declined_only` keeps exactly it, `renderable_only` keeps exactly
/// the complement, and it still renders in the Exasol dialect — so the outer
/// wrapper's WHERE can apply what no leg can.
#[test]
fn declined_side_local_conjunct_partitions_to_residual() {
    let filter = orders_local_rendering_and_declined_filter();
    let rendering = orders_local_rendering_conjunct();
    let declined = orders_local_declined_conjunct();
    assert!(
        !datafusion_renderable(&declined) && datafusion_renderable(&rendering),
        "precondition: exactly one of the two conjuncts declines for DataFusion"
    );

    assert_eq!(
        declined_only(&filter),
        Some(declined.clone()),
        "declined_only must keep exactly the conjunct DataFusion cannot express"
    );
    assert_eq!(
        renderable_only(&filter),
        Some(rendering),
        "renderable_only must keep exactly its complement — the two are exact halves"
    );
    assert!(
        render_expression_exasol_safe(&declined).is_some(),
        "the residual conjunct must render in the Exasol dialect, or the outer \
         WHERE could not apply it either"
    );
    assert_eq!(
        conjunct_single_side(&declined).as_deref(),
        Some("ORDERS"),
        "attribution is unchanged — only the RENDER declines, so the screen is \
         the sole reason this conjunct becomes residual"
    );
}

/// The complement: a side-local conjunct the DataFusion dialect CAN express
/// still reaches its own leg through the screened tree, so the screen costs the
/// rendering case nothing.
#[test]
fn rendering_side_local_conjunct_still_reaches_its_leg() {
    let filter = orders_local_rendering_and_declined_filter();
    let leg_eligible = renderable_only(&filter).expect("the rendering conjunct survives");

    let leg = render_df_filter_safe(
        &side_local_filter(&leg_eligible, "ORDERS").expect("still ORDERS-side-local"),
    )
    .expect("a DataFusion-renderable leg filter renders");

    assert!(
        leg.contains("'1995-01-01'") && !leg.contains("SECOND"),
        "the rendering conjunct must reach the leg and the declined one must not: {leg}"
    );
}

/// The Iceberg manifest-pruning input is NOT screened: `plan_join` passes the
/// RAW filter to `side_local_filter`, so a conjunct whose DataFusion render
/// declines still prunes that side's manifests. Only the leg's `ScanSpec.filter`
/// sees the screened tree — screening inside `side_local_filter` would silently
/// open more files with no failing test.
#[test]
fn join_side_pruning_input_unchanged_when_df_render_declines() {
    let filter = orders_local_rendering_and_declined_filter();

    let pruning =
        side_local_filter(&filter, "ORDERS").expect("both conjuncts are ORDERS-side-local");
    let mut pruning_conjuncts = Vec::new();
    flatten_conjuncts(&pruning, &mut pruning_conjuncts);
    assert_eq!(
        pruning_conjuncts.len(),
        2,
        "pruning must still receive BOTH side-local conjuncts: {pruning}"
    );
    assert!(
        pruning_conjuncts.iter().any(|c| !datafusion_renderable(c)),
        "the declined conjunct must still be in the pruning input: {pruning}"
    );

    let leg = side_local_filter(
        &renderable_only(&filter).expect("the rendering conjunct survives"),
        "ORDERS",
    )
    .expect("the rendering conjunct is still ORDERS-side-local");
    let mut leg_conjuncts = Vec::new();
    flatten_conjuncts(&leg, &mut leg_conjuncts);
    assert_eq!(
        leg_conjuncts.len(),
        1,
        "the screened leg filter must carry only the rendering conjunct: {leg}"
    );
    assert!(
        leg_conjuncts.iter().all(|c| datafusion_renderable(c)),
        "the screened leg filter must omit the declined conjunct: {leg}"
    );
}

/// CUSTOMER's `(name, Exasol type)` universe from the standard two-table join
/// request: `C_CUSTKEY DECIMAL(20,0)`, `C_NAME VARCHAR(100)`.
fn customer_col_types() -> Vec<(String, String)> {
    involved_table_columns(&join_request(Json::Null, equi_condition()), "CUSTOMER")
}

/// ORDERS' `(name, Exasol type)` universe: `O_CUSTKEY DECIMAL(20,0)`,
/// `O_ORDERDATE DATE` — one type the LIKE guard declines and one it rewrites.
fn orders_col_types() -> Vec<(String, String)> {
    involved_table_columns(&join_request(Json::Null, equi_condition()), "ORDERS")
}

fn like_over(column: &str, table: &str, pattern: &str) -> Json {
    serde_json::json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": column, "tableName": table},
        "pattern": {"type": "literal_string", "value": pattern}
    })
}

fn and_of(conjuncts: Vec<Json>) -> Json {
    serde_json::json!({"type": "predicate_and", "expressions": conjuncts})
}

fn conjunct_count(filter: Option<&Json>) -> usize {
    filter.map_or(0, |f| {
        let mut out = Vec::new();
        flatten_conjuncts(f, &mut out);
        out.len()
    })
}

/// Every conjunct the type pipeline accepts reaches the leg half REWRITTEN and the
/// declined half stays empty — so an all-accepted side keeps its full pushdown, and
/// the leg receives the rewritten tree (`CAST("O_ORDERDATE" AS VARCHAR) LIKE …`),
/// not the raw one DataFusion would refuse to coerce.
#[test]
fn type_screened_leg_filter_pushes_whole_accepted_set_rewritten() {
    let filter = and_of(vec![
        like_over("O_ORDERDATE", "ORDERS", "1995%"),
        orders_local_rendering_conjunct(),
    ]);

    let (leg, declined) = type_screened_leg_filter(&filter, &orders_col_types());

    assert!(
        declined.is_none(),
        "no conjunct declines, so nothing may be handed to the outer wrapper: {declined:?}"
    );
    let leg = leg.expect("an all-accepted side-local set must reach its leg");
    assert_eq!(
        conjunct_count(Some(&leg)),
        2,
        "both accepted conjuncts must reach the leg: {leg}"
    );
    let rendered = render_df_filter_safe(&leg).expect("the rewritten set must render");
    assert!(
        rendered.contains(r#"CAST("O_ORDERDATE" AS VARCHAR)"#) && rendered.contains("LIKE"),
        "the DATE LIKE subject must reach the leg CAST to VARCHAR, not bare: {rendered}"
    );
}

/// When no conjunct survives the screen the leg half is `None` and the outer
/// wrapper receives the WHOLE side-local set in RAW form — the DECIMAL LIKE
/// unwrapped, because the Exasol dialect must render what the DataFusion dialect
/// declined, and the rewrites target the DataFusion dialect only.
#[test]
fn type_screened_leg_filter_declines_whole_set_when_no_conjunct_survives() {
    let decimal_like = like_over("O_CUSTKEY", "ORDERS", "1%");
    let filter = and_of(vec![decimal_like.clone(), orders_local_declined_conjunct()]);

    let (leg, declined) = type_screened_leg_filter(&filter, &orders_col_types());

    assert!(
        leg.is_none(),
        "no conjunct may be pushed when every one of them declines: {leg:?}"
    );
    let declined = declined.expect("a fully declined side-local set must go residual");
    let mut conjuncts = Vec::new();
    flatten_conjuncts(&declined, &mut conjuncts);
    assert_eq!(
        conjuncts.len(),
        2,
        "both declined conjuncts must reach the outer wrapper: {declined}"
    );
    assert!(
        conjuncts.contains(&&decimal_like),
        "the DECIMAL LIKE must be carried RAW so the Exasol dialect can render it: \
         {declined}"
    );
}

/// The partition is TOTAL and DISJOINT over the side-local conjuncts in every
/// shape — all accepted, all declined, mixed — and it FAILS CLOSED: whenever the
/// leg half is absent the declined half carries every conjunct, so a predicate is
/// never dropped from both halves (that would return wrong rows, where a residual
/// conjunct is merely slower).
#[test]
fn type_screened_leg_filter_partition_is_total_and_fails_closed() {
    let date_like = like_over("O_ORDERDATE", "ORDERS", "1995%");
    let decimal_like = like_over("O_CUSTKEY", "ORDERS", "1%");
    let mixed = and_of(vec![
        date_like.clone(),
        decimal_like.clone(),
        orders_local_rendering_conjunct(),
    ]);
    let cases = [
        (
            "all accepted",
            and_of(vec![date_like, orders_local_rendering_conjunct()]),
        ),
        (
            "all declined",
            and_of(vec![decimal_like.clone(), orders_local_declined_conjunct()]),
        ),
        ("mixed", mixed.clone()),
    ];

    for (label, filter) in cases {
        let input = conjunct_count(Some(&filter));
        let (leg, declined) = type_screened_leg_filter(&filter, &orders_col_types());
        assert_eq!(
            conjunct_count(leg.as_ref()) + conjunct_count(declined.as_ref()),
            input,
            "{label}: every conjunct must land in exactly one half — none dropped, \
             none double-applied"
        );
        if leg.is_none() {
            assert_eq!(
                conjunct_count(declined.as_ref()),
                input,
                "{label}: fail closed — with no leg half the declined half must carry \
                 the WHOLE side-local set"
            );
        }
    }

    let (leg, declined) = type_screened_leg_filter(&mixed, &orders_col_types());
    let leg_sql = render_df_filter_safe(&leg.expect("the two accepted conjuncts form a leg"))
        .expect("the rewritten accepted set must render for DataFusion");
    assert!(
        leg_sql.contains(r#"CAST("O_ORDERDATE" AS VARCHAR)"#) && !leg_sql.contains("O_CUSTKEY"),
        "one type-declining conjunct must not forfeit its side's other pushable \
         conjuncts: {leg_sql}"
    );
    assert_eq!(
        declined,
        Some(decimal_like),
        "the declined half must be exactly the type-declined conjunct, RAW"
    );
}

/// A conjunct the type pipeline ACCEPTS but the DataFusion dialect cannot render
/// lands in the DECLINED half in RAW form, never dropped from both: the leg renders
/// the REWRITTEN tree, so that is the tree whose renderability decides the
/// partition. `SECOND(CAST(<DECIMAL col> AS VARCHAR), 3)` is exactly that shape —
/// the decimal pass rewrites the cast, and the two-argument `SECOND` arity is one
/// the DataFusion dialect refuses.
#[test]
fn type_screened_leg_filter_declines_type_accepted_but_unrenderable_rewrite() {
    let col_types = customer_col_types();
    let filter = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "function_scalar", "name": "SECOND", "arguments": [
            {"type": "function_scalar_cast", "name": "CAST",
             "dataType": {"type": "VARCHAR", "size": 40},
             "arguments": [
                 {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"}]},
            {"type": "literal_exactnumeric", "value": 3}
        ]},
        "right": {"type": "literal_exactnumeric", "value": 1}
    });

    let rewritten = apply_type_rewrites(&filter, &col_types)
        .expect("precondition: the type pipeline ACCEPTS this tree");
    assert!(
        rewritten.to_string().contains("decimal_to_varchar_exasol"),
        "precondition: the pipeline REWRITES the DECIMAL stringification: {rewritten}"
    );
    assert!(
        !datafusion_renderable(&rewritten),
        "precondition: the REWRITTEN tree still does not render for DataFusion"
    );

    let (leg, declined) = type_screened_leg_filter(&filter, &col_types);

    assert!(
        leg.is_none(),
        "a rewrite the DataFusion dialect cannot render must not reach the leg: {leg:?}"
    );
    assert_eq!(
        declined,
        Some(filter),
        "it must reach the outer wrapper RAW — with the plain CAST, not the \
         DataFusion-only rewrite the Exasol dialect has no node for"
    );
}

/// The N-scan path has NO disjoint-column-name precondition, so each side is
/// screened against ITS OWN `col_types`: the same bare `SHARED_KEY` LIKE is
/// accepted (and CAST) on the side declaring it `DATE` and declined on the side
/// declaring it `DECIMAL`. A shared union would resolve one name to two types and
/// screen at least one side against the wrong one.
#[test]
fn type_screened_leg_filter_uses_owning_side_types_for_shared_column_name() {
    let request = serde_json::json!({
        "involvedTables": [
            {"name": "DATED", "columns": [
                {"name": "SHARED_KEY", "dataType": {"type": "date"}}]},
            {"name": "NUMBERED", "columns": [
                {"name": "SHARED_KEY",
                 "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
        ]
    });
    let filter = like_over("SHARED_KEY", "DATED", "1995%");

    let (date_leg, date_declined) =
        type_screened_leg_filter(&filter, &involved_table_columns(&request, "DATED"));
    let date_sql = render_df_filter_safe(
        &date_leg.expect("the DATE side accepts the LIKE through the CAST rewrite"),
    )
    .expect("the CAST-rewrapped LIKE renders for DataFusion");
    assert!(
        date_sql.contains(r#"CAST("SHARED_KEY" AS VARCHAR)"#),
        "the DATE side must push the CAST-rewritten LIKE into its leg: {date_sql}"
    );
    assert!(
        date_declined.is_none(),
        "the DATE side declines nothing: {date_declined:?}"
    );

    let (numeric_leg, numeric_declined) =
        type_screened_leg_filter(&filter, &involved_table_columns(&request, "NUMBERED"));
    assert!(
        numeric_leg.is_none(),
        "the DECIMAL side has no safe rewrite, so nothing may reach its leg: \
         {numeric_leg:?}"
    );
    assert_eq!(
        numeric_declined,
        Some(filter),
        "the DECIMAL side must hand the SAME conjunct to the outer wrapper RAW"
    );
}

/// The fallback projection is narrowed to the columns the outer wrapper
/// references for a side — SELECT list + join condition + WHERE — preserving
/// the full-column order/type, and dropping columns referenced nowhere.
#[test]
fn referenced_side_columns_narrows_to_used_columns() {
    let pushdown_req = serde_json::json!({
        "selectList": [{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}],
        "filter": {"type": "predicate_equal",
            "left": {"type": "column", "name": "C_ADDRESS", "tableName": "CUSTOMER"},
            "right": {"type": "literal_string", "value": "z"}},
    });
    let condition = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
        "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}
    });
    let full = vec![
        ("C_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
        ("C_NAME".to_string(), "VARCHAR(100)".to_string()),
        ("C_ADDRESS".to_string(), "VARCHAR(100)".to_string()),
        ("C_PHONE".to_string(), "VARCHAR(20)".to_string()),
    ];
    let narrowed = referenced_side_columns(&pushdown_req, &condition, "CUSTOMER", &full);
    let names: Vec<&str> = narrowed.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        vec!["C_CUSTKEY", "C_NAME", "C_ADDRESS"],
        "narrows to condition + select + filter columns, in full-column order, dropping C_PHONE"
    );
    // The kept columns retain their full-column Exasol types.
    assert_eq!(
        narrowed[1],
        ("C_NAME".to_string(), "VARCHAR(100)".to_string())
    );
}

/// An absent (or empty) SELECT list means the wrapper projects every column via
/// `SELECT *`, so no narrowing is applied — all columns are kept.
#[test]
fn referenced_side_columns_keeps_all_when_select_list_absent() {
    let condition = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
        "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}
    });
    let full = vec![
        ("C_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
        ("C_NAME".to_string(), "VARCHAR(100)".to_string()),
    ];
    let narrowed = referenced_side_columns(&serde_json::json!({}), &condition, "CUSTOMER", &full);
    assert_eq!(
        narrowed, full,
        "an absent select list ⇒ SELECT *, keep every column"
    );
}

/// A narrowing that selects no column of this side keeps the FULL column set —
/// `referenced_side_columns` never emits a zero-column fan-out leg. That full-set
/// fallback is its own policy; `referenced_column_projection` falls back to only
/// the first column instead, and the two MUST stay divergent.
#[test]
fn referenced_side_columns_keeps_all_when_narrowing_empty() {
    let pushdown_req = serde_json::json!({
        "selectList": [{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}],
    });
    let condition = equi_condition();
    let full = vec![
        ("L_ORDERKEY".to_string(), "DECIMAL(20,0)".to_string()),
        ("L_QUANTITY".to_string(), "DECIMAL(18,2)".to_string()),
    ];
    let narrowed = referenced_side_columns(&pushdown_req, &condition, "LINEITEM", &full);
    assert_eq!(
        narrowed, full,
        "no clause references a LINEITEM column ⇒ keep every column rather than \
         emit a zero-column leg"
    );
}

/// The two column collectors MUST keep their divergent case folding:
/// `collect_all_column_names` folds with Unicode `to_uppercase`,
/// `collect_side_column_names` with ASCII-only `to_ascii_uppercase`. `ß` is the
/// witness — Unicode folds it to `SS`, ASCII leaves it untouched. No other test in
/// this crate uses a non-ASCII identifier, so without this test reconciling the two
/// folds (which sharing one clause walk invites) would change behavior while the
/// whole suite still passed.
#[test]
fn column_collectors_keep_divergent_case_folding() {
    let expr = serde_json::json!({
        "type": "column", "name": "straße", "tableName": "CUSTOMER",
    });

    let mut unicode_folded = std::collections::HashSet::new();
    collect_all_column_names(&expr, &mut unicode_folded);
    assert_eq!(
        unicode_folded,
        std::collections::HashSet::from(["STRASSE".to_string()]),
        "collect_all_column_names folds ß to SS via Unicode to_uppercase"
    );

    let mut ascii_folded = std::collections::HashSet::new();
    collect_side_column_names(&expr, "CUSTOMER", &mut ascii_folded);
    assert_eq!(
        ascii_folded,
        std::collections::HashSet::from(["STRAßE".to_string()]),
        "collect_side_column_names leaves ß untouched via to_ascii_uppercase"
    );

    assert_ne!(
        unicode_folded, ascii_folded,
        "the two folds are NOT interchangeable and MUST NOT be unified"
    );
}

/// A per-side fan-out pushes its side-local filter down as a DataFusion
/// `ScanSpec.filter` (present in the common blob); absent when there is none.
///
/// Exasol sends each column with a `tableAlias` (the query's `FROM fact_orders o`
/// alias). The fan-out is a SINGLE-TABLE scan over a relation with BARE
/// uppercase columns, so its pushed filter MUST render bare — the alias must be
/// stripped, or the alias-qualified reference fails to resolve against the
/// fan-out.
#[test]
fn side_fan_out_pushes_bare_side_local_filter_into_common_blob() {
    let side = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]);
    let cols = vec![
        ("O_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
        ("O_ORDERDATE".to_string(), "DATE".to_string()),
    ];
    // Exactly the Exasol shape: BOTH tableName AND tableAlias present.
    let filter = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS", "tableAlias": "O"},
        "right": {"type": "literal_string", "value": "1995-01-01"}
    });

    let sql_with = build_side_fan_out_sql(
        &side,
        &cols,
        Some(&filter),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    );
    let common = common_arg_literal(&sql_with);
    assert!(
        common.contains("\"filter\"") && common.contains("O_ORDERDATE"),
        "the side-local filter must be pushed into the fan-out common blob: {common}"
    );
    assert!(
        !common.contains(r#"\"O\".\"O_ORDERDATE\""#) && !common.contains(r#""O"."O_ORDERDATE""#),
        "the fan-out filter MUST be bare (alias stripped), never alias-qualified: {common}"
    );

    let sql_without =
        build_side_fan_out_sql(&side, &cols, None, &two_scan_tuning(), "SCAN", "DISTRIBUTE");
    let common_none = common_arg_literal(&sql_without);
    assert!(
        !common_none.contains("\"filter\""),
        "no side-local filter ⇒ no filter field in the common blob: {common_none}"
    );
}

/// A multi-shard join leg routes through the distributor + scalar scan
/// primitive: the fan-out `GROUP BY shard_key` lives in the distributor and the
/// outer scalar `SCAN` is ungrouped, with NO `SELECT * FROM (...)` materialization
/// wrapper. The leg is a bare subquery the outer join wrapper reads.
#[test]
fn side_fan_out_routes_through_distributor_scalar_scan_no_wrapper() {
    let side = resolved_side(
        "ORDERS",
        vec![("s3://w/o-0.parquet", 100), ("s3://w/o-1.parquet", 100)],
    );
    let cols = vec![("O_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string())];
    // Force two shards: two nodes × factor 1 over two files.
    let tuning = JoinScanTuning {
        cluster_nodes: 2,
        parallelism_factor: 1,
        ..two_scan_tuning()
    };
    let sql = build_side_fan_out_sql(&side, &cols, None, &tuning, "SCAN", "DISTRIBUTE");

    assert!(
        !sql.contains("SELECT * FROM ("),
        "the leg must not use a SELECT * materialization wrapper: {sql}"
    );
    assert!(
        sql.starts_with("SELECT SCAN("),
        "the leg is the outer ungrouped scalar scan itself: {sql}"
    );
    assert!(
        sql.contains("DISTRIBUTE(files) FROM (VALUES")
            && sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
        "the leg's fan-out GROUP BY shard_key must live in the distributor: {sql}"
    );
}

/// The broadcast fact side routes through the same distributor + scalar scan
/// primitive: a multi-file fact side fans out via the nested distributor under
/// an outer ungrouped scalar `SCAN`, with no `SELECT * FROM (...)` wrapper; the
/// dimension side rides once in the common blob's join block.
#[test]
fn broadcast_fact_side_uses_distributor_scalar_scan() {
    let fact = resolved_side(
        "LINEITEM",
        vec![("s3://w/l-0.parquet", 1000), ("s3://w/l-1.parquet", 1000)],
    );
    let dimension = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]);
    let sides = JoinSides {
        fact,
        dimension,
        broadcast_eligible: true,
    };
    let rendered = RenderedJoinPushdown {
        condition: r#""L_ORDERKEY" = "O_ORDERKEY""#.to_string(),
        filter: None,
        projection: vec![ProjectionItem::Column("L_ORDERKEY".to_string())],
        projection_types: vec!["DECIMAL(20,0)".to_string()],
    };
    let tuning = JoinScanTuning {
        cluster_nodes: 2,
        parallelism_factor: 1,
        ..two_scan_tuning()
    };
    let sql = build_broadcast_join_sql(&sides, &rendered, &tuning, "SCAN", "DISTRIBUTE");

    assert!(
        !sql.contains("SELECT * FROM ("),
        "the broadcast fact side must not use a SELECT * wrapper: {sql}"
    );
    assert!(
        sql.starts_with("SELECT SCAN("),
        "the fact side is the outer ungrouped scalar scan itself: {sql}"
    );
    assert!(
        sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
        "the fact side fans out via the nested shard_key distributor: {sql}"
    );
}

/// The broadcast path strips Exasol's native `tableAlias` qualifier before
/// rendering `rendered.filter`: `build_join_sql` wraps each side in an
/// UNALIASED derived sub-SELECT, so a preserved alias would not resolve
/// against it (`No field named "O"."O_ORDERDATE"`).
#[test]
fn render_broadcast_join_strips_native_table_alias_from_filter() {
    let mut request = join_request(Json::Null, equi_condition());
    // Give every join column node Exasol's native tableAlias, as the live cluster does.
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS", "tableAlias": "O"},
        "right": {"type": "literal_string", "value": "1995-01-01"}
    });
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("renders")
        .expect("disjoint join renders");
    let filter = rendered.filter.expect("filter renders");
    assert!(
        filter.contains(r#""O_ORDERDATE""#),
        "the filter must still reference the column: {filter}"
    );
    assert!(
        !filter.contains(r#""O"."O_ORDERDATE""#),
        "broadcast rendering must strip Exasol's native tableAlias (bare, unqualified): {filter}"
    );
}

/// An equi-condition whose two column nodes both carry Exasol's native
/// `tableAlias` — the exact shape of the live defect — renders bare, matching
/// the unaliased shape `build_join_sql`'s derived sub-SELECTs expose.
#[test]
fn render_broadcast_join_strips_native_table_alias_from_condition() {
    let condition = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER", "tableAlias": "C"},
        "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS", "tableAlias": "O"},
    });
    let request = join_request(Json::Null, condition);
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("disjoint, renderable join")
        .expect("a disjoint join must render, not decline");
    assert_eq!(rendered.condition, r#"("C_CUSTKEY" = "O_CUSTKEY")"#);
}

/// A select-list scalar expression over an aliased column renders its
/// `ProjectionItem::Expr` bare, not alias-qualified — `extract_join_projection`
/// also renders via `render_expression_safe`, so it needs the same stripping.
#[test]
fn render_broadcast_join_strips_native_table_alias_from_projection() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["selectList"] = serde_json::json!([
        {"type": "function_scalar", "name": "MULT", "arguments": [
            {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS", "tableAlias": "O"},
            {"type": "literal_exactnumeric", "value": 2}
        ]}
    ]);
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(&request, &pd(&request), &detected)
        .expect("renders")
        .expect("disjoint join renders");
    assert_eq!(rendered.projection.len(), 1, "{:?}", rendered.projection);
    match &rendered.projection[0] {
        ProjectionItem::Expr { expr } => {
            assert_eq!(expr, r#"("O_CUSTKEY" * 2)"#);
        }
        other => panic!("expected an Expr projection item, got {other:?}"),
    }
}

/// End-to-end fallback wiring: the unified wrapper prunes each leg (side-local
/// filter pushed into BOTH fan-out common blobs) AND narrows each leg's
/// projection (an involved column referenced nowhere in the wrapper is dropped).
/// Here BOTH filter conjuncts are side-local (one per leg), so the outer WHERE
/// has no residual conjunct and is omitted entirely; the join condition attaches
/// to the INNER JOIN's ON clause instead.
#[test]
fn unified_join_prunes_and_narrows_each_leg() {
    let request = serde_json::json!({
        "involvedTables": [
            {"name": "CUSTOMER", "columns": [
                {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
                {"name": "C_ADDRESS", "dataType": {"type": "varchar", "size": 100}}]},
            {"name": "ORDERS", "columns": [
                {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "O_ORDERDATE", "dataType": {"type": "date"}},
                {"name": "O_TOTALPRICE", "dataType": {"type": "decimal", "precision": 20, "scale": 2}}]},
        ],
        "pushdownRequest": {
            "type": "select",
            "from": {"type": "join", "join_type": "inner",
                "left": {"name": "CUSTOMER", "type": "table"},
                "right": {"name": "ORDERS", "type": "table"},
                "condition": {"type": "predicate_equal",
                    "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                    "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}}},
            "selectList": [
                {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}],
            "filter": {"type": "predicate_and", "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 "right": {"type": "literal_string", "value": "1995-01-01"}}]},
        },
        "schemaMetadataInfo": {"properties": {}, "adapterNotes":
            serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}})
                .to_string()},
    });

    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &request,
        &pd(&request),
        &detected,
        &sides,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("unified wrapper must build");

    // Columns referenced nowhere in the wrapper are dropped from the legs.
    assert!(
        !sql.contains("C_ADDRESS"),
        "an unreferenced CUSTOMER column must be narrowed out of the fan-out: {sql}"
    );
    assert!(
        !sql.contains("O_TOTALPRICE"),
        "an unreferenced ORDERS column must be narrowed out of the fan-out: {sql}"
    );

    // Each leg gets its own side-local filter pushed into its common blob.
    assert_eq!(
        sql.matches("\"filter\"").count(),
        2,
        "both fan-out legs must carry a side-local ScanSpec.filter: {sql}"
    );

    // Both side-local conjuncts are pushed into their legs' common blobs; the
    // outer WHERE keeps only cross-table residual, of which there is none here.
    assert!(
        sql.contains("'ACME'") && sql.contains("'1995-01-01'"),
        "each leg's side-local conjunct must be pushed into its fan-out: {sql}"
    );
    assert!(
        !sql.contains(" WHERE "),
        "no cross-table residual conjunct remains, so the outer WHERE is omitted: {sql}"
    );
    // The join condition attaches to the INNER JOIN chain's ON clause.
    assert!(
        sql.contains(r#"ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
        "the equi-condition attaches to the join point's ON clause: {sql}"
    );
}
