use super::super::single_group_agg::{single_group_merge_select, single_group_plan_types};
use super::super::support::{
    AggregateMergeInputs, DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, build_scan_driving_sql,
    classify_where_filter, extract_all_column_types, extract_projection, order_by_present,
    shard_count,
};
use super::super::test_support::*;
use super::super::{detect_aggregates, ordinary_plans, validate_agg_col_types};
use super::*;
use crate::scan::spec::{CommonScanSpec, FileEntry, ScanSpec, ScanStorage};

// -----------------------------------------------------------------------
// Ordered top-N pushdown (B3)
// -----------------------------------------------------------------------

/// Reproduce `handle_pushdown`'s SYNCHRONOUS row-scan decision path (everything
/// after resolution) so tests exercise the real `detect_topn`,
/// `effective_limit` withholding glue, and `build_scan_driving_sql` — the exact
/// composition production runs, minus the network file resolution.
fn plan_scan_sql(request: &Json, files: Vec<(String, u64)>, cluster_nodes: usize) -> String {
    let pushdown_req = request
        .get("pushdownRequest")
        .cloned()
        .unwrap_or(Json::Null);
    let (mut proj_cols, mut proj_types, widened) =
        extract_projection(request, &pushdown_req).unwrap();
    let limit = extract_limit(&pushdown_req);
    let has_order_by = order_by_present(&pushdown_req);
    let col_types = extract_all_column_types(request);
    // Production classifies the WHERE filter ONCE through `classify_where_filter`
    // and routes a DECLINED one to the qualified single-table wrapper AHEAD of the
    // routing classifier. This mirror reproduces the classification but covers only
    // the no-decline half of it — the shape whose scan spec carries the filter — so
    // a declining fixture belongs on `build_dispatch_sql`, which owns that route.
    let (filter, declined_filter) = classify_where_filter(
        pushdown_req.get("filter").filter(|f| !f.is_null()),
        &col_types,
    );
    assert!(
        declined_filter.is_none(),
        "plan_scan_sql mirrors only the no-decline dispatch path; a declined-filter fixture needs build_dispatch_sql, not this helper"
    );

    let items = detect_aggregates(&pushdown_req)
        .filter(|it| validate_agg_col_types(&ordinary_plans(it), &col_types));
    // Mirrors the dispatcher's single-group aggregate inputs: the folded plans, their
    // per-plan declared `EMITS` types, and the caller-owned merge SELECT.
    let merge_inputs = items.as_deref().map(|it| {
        let plans = ordinary_plans(it);
        let plan_types = single_group_plan_types(&pushdown_req, it);
        let merge_select = single_group_merge_select(it, &plans, &plan_types)
            .expect("plan_scan_sql mirrors only fixtures whose merge SELECT assembles in full");
        AggregateMergeInputs::new(plan_types, merge_select, limit)
            .expect("one merge item per select-list item is never empty")
    });
    let aggregates = items.map(|it| ordinary_plans(&it));
    // Production routes a widened projection to the qualified single-table
    // wrapper ONLY from the `RequestShape::RowScan` arm (`mod.rs`'s
    // `if projection_widened` sits inside it). An aggregate select list ALWAYS
    // widens — `project_columns` keeps aggregates off the projection — and never
    // reaches that guard, so the mirror must accept it on the aggregate path.
    assert!(
        !widened || aggregates.is_some(),
        "plan_scan_sql mirrors only the non-widened dispatch path; a widened row-scan fixture needs build_dispatch_sql, not this helper"
    );
    // Production always resolves a logical schema before detect_topn; reproduce
    // the LINEITEM schema every plan_scan_sql caller's request scans over.
    let logical_schema = lineitem_logical_schema();
    let topn = if aggregates.is_none() {
        detect_topn(request, &pushdown_req, &proj_cols, &logical_schema)
    } else {
        None
    };
    let order_by = topn.unwrap_or_default();
    let effective_limit = if has_order_by && order_by.is_empty() {
        None
    } else {
        limit
    };

    // Row-scan DECLINE path via the SHARED helpers the dispatcher calls, so this
    // mirror cannot drift from the real wrapping shape. Position is load-bearing on
    // both sides, exactly as in `build_dispatch_sql`: AFTER `detect_topn` (which
    // must see the pre-extension projection) and BEFORE the `spec_template` below
    // (whose `projection` / `emit_exa_types` must carry the appended hidden column
    // that the EMITS clause is built from).
    let visible_count = proj_cols.len();
    let declined_order_by = has_order_by && order_by.is_empty() && aggregates.is_none();
    let declined_sort_keys = if declined_order_by {
        let keys = parse_order_by_keys(&pushdown_req);
        // Mirrors the dispatcher's correctness-safety guard at the same position
        // (#198). Every fixture routed through this helper renders in full; a
        // declining one belongs on `build_dispatch_sql`, which returns the error.
        ensure_every_sort_key_renders(&keys)
            .expect("plan_scan_sql mirrors only fixtures whose pushed ORDER BY renders in full");
        extend_projection_with_sort_keys(&mut proj_cols, &mut proj_types, &keys, &col_types);
        keys
    } else {
        Vec::new()
    };

    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: proj_cols.clone(),
            filter,
            limit: effective_limit,
            order_by,
            aggregates,
            emit_exa_types: proj_types.clone(),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let files: Vec<FileEntry> = files.into_iter().map(FileEntry::from).collect();
    let g = shard_count(cluster_nodes, 1, files.len());
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &proj_cols,
        &proj_types,
        effective_limit,
        &col_types,
        merge_inputs.as_ref(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    );
    if declined_order_by {
        wrap_declined_order_by(
            &sql,
            &proj_cols,
            visible_count,
            &declined_sort_keys,
            limit,
            extract_offset(&pushdown_req),
        )
    } else {
        sql
    }
}

/// The logical schema production resolves for the NQ4 (LINEITEM) requests: both
/// sort-eligible columns are in-range DECIMALs, so neither needs the JSON
/// fallback and `detect_topn` matches. Field-ids are illustrative.
fn lineitem_logical_schema() -> Vec<LogicalField> {
    vec![
        LogicalField {
            field_id: Some(1),
            name: "L_ORDERKEY".into(),
            arrow_type: "decimal128(20,0)".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "L_EXTENDEDPRICE".into(),
            arrow_type: "decimal128(18,2)".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ]
}

/// [`parse_sort_flags`] reads direction + NULL placement off ANY `orderBy`
/// element, with no column-node requirement, so an expression sort key can reach
/// the shared `render_ordered` seam. [`parse_sort_key_element`]'s bare-column gate
/// is untouched by it — the same expression element still yields no [`SortKey`],
/// which is what keeps [`detect_topn`] eligibility unchanged.
#[test]
fn parse_sort_flags_reads_direction_and_nulls_without_column_gate() {
    let expression_element = serde_json::json!({
        "type": "order_by_element",
        "expression": {"type": "function_scalar", "name": "ABS", "arguments": [
            {"type": "column", "name": "L_EXTENDEDPRICE"}
        ]},
        "isAscending": false,
        "nullsLast": true
    });
    assert_eq!(
        parse_sort_flags(&expression_element),
        Some((false, true)),
        "an expression element's flags must parse"
    );
    assert!(
        parse_sort_key_element(&expression_element).is_none(),
        "the bare-column gate must still reject the same element"
    );

    let column_element = serde_json::json!({
        "type": "order_by_element",
        "expression": {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
        "isAscending": true,
        "nullsLast": false
    });
    assert_eq!(parse_sort_flags(&column_element), Some((true, false)));

    // A missing flag is an unexpected shape on either side: no default is invented.
    for missing in ["isAscending", "nullsLast"] {
        let mut partial = expression_element.clone();
        partial.as_object_mut().unwrap().remove(missing);
        assert_eq!(
            parse_sort_flags(&partial),
            None,
            "a missing {missing} must not be defaulted"
        );
    }
}

/// Match: the ordered top-N wraps the fan-out in an outer `ORDER BY … LIMIT n`
/// and carries the SAME sort keys + limit into the shard-invariant common blob
/// (which the scan UDF renders as the per-shard bounded sort). Multi-shard so a
/// real fan-out + merge is exercised.
#[test]
fn ordered_topn_emits_per_shard_and_outer_order_by() {
    let request = nq4_request();
    let files = vec![
        ("s3://w/part-0.parquet".to_string(), 1000u64),
        ("s3://w/part-1.parquet".to_string(), 1000u64),
    ];
    // Two nodes → two shards → a genuine GROUP BY shard_key fan-out.
    let sql = plan_scan_sql(&request, files, 2);

    // Outer merge ORDER BY, explicit direction + NULL placement, before LIMIT.
    assert!(
        sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20"#),
        "matched top-N must render an outer ORDER BY … LIMIT: {sql}"
    );
    // The per-shard common blob carries the identical sort keys AND the limit,
    // so every shard runs the same bounded sort (rendered by the scan UDF).
    let common = common_arg_literal(&sql);
    assert!(
        common.contains(
            r#""order_by":[{"column":"L_EXTENDEDPRICE","ascending":false,"nulls_last":true}]"#
        ),
        "common blob must carry the per-shard sort keys: {common}"
    );
    assert!(
        common.contains(r#""limit":20"#),
        "common blob must carry the per-shard limit: {common}"
    );
}

/// A NON-ZERO `limit.offset` DECLINES the bounded per-shard top-N, and the window
/// is rendered ONCE — on the declined wrapper, beside the `ORDER BY` it renders
/// itself: `ORDER BY … LIMIT n OFFSET m` (issue #191). A per-shard
/// `LIMIT n OFFSET m` would skip each shard's OWN first m rows and does not
/// compose, so the fan-out stays unbounded and unsorted.
#[test]
fn nonzero_offset_declines_bounded_topn() {
    let mut request = nq4_request();
    request["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 20, "offset": 5});
    let projected = vec![
        ProjectionItem::Column("L_ORDERKEY".into()),
        ProjectionItem::Column("L_EXTENDEDPRICE".into()),
    ];
    assert!(
        detect_topn(
            &request,
            &pd(&request),
            &projected,
            &lineitem_logical_schema()
        )
        .is_none(),
        "a non-zero offset must decline the bounded top-N path"
    );

    let files = vec![
        ("s3://w/part-0.parquet".to_string(), 1000u64),
        ("s3://w/part-1.parquet".to_string(), 1000u64),
    ];
    let sql = plan_scan_sql(&request, files, 2);

    assert!(
        sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20 OFFSET 5"#),
        "the declined wrapper must render the full window beside its ORDER BY: {sql}"
    );
    assert_eq!(
        sql.matches("OFFSET").count(),
        1,
        "the offset belongs on the wrapper alone, never in the fan-out: {sql}"
    );
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("\"limit\"") && !common.contains("order_by"),
        "the per-shard fan-out must carry neither the limit nor the sort keys: {common}"
    );
}

/// `offset: 0` is the SAME request as an ABSENT `offset` key (Exasol normalises an
/// explicit `OFFSET 0` away), so it must still MATCH the bounded top-N and yield
/// byte-identical SQL: the guard is a non-zero test, not a presence test. A
/// presence test behaves identically on today's Exasol but would silently decline
/// every ordered LIMIT query cluster-wide on a future build that does attach
/// `offset: 0`.
#[test]
fn zero_offset_still_matches_bounded_topn_byte_identically() {
    let baseline = nq4_request();
    let mut zero_offset = nq4_request();
    zero_offset["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 20, "offset": 0});
    let projected = vec![
        ProjectionItem::Column("L_ORDERKEY".into()),
        ProjectionItem::Column("L_EXTENDEDPRICE".into()),
    ];
    let matched = detect_topn(
        &baseline,
        &pd(&baseline),
        &projected,
        &lineitem_logical_schema(),
    );
    assert!(matched.is_some(), "sanity: the baseline shape must match");
    assert_eq!(
        detect_topn(
            &zero_offset,
            &pd(&zero_offset),
            &projected,
            &lineitem_logical_schema()
        ),
        matched,
        "offset 0 must match the bounded top-N exactly as an absent offset does"
    );

    let files = || {
        vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ]
    };
    let baseline_sql = plan_scan_sql(&baseline, files(), 2);
    assert_eq!(
        plan_scan_sql(&zero_offset, files(), 2),
        baseline_sql,
        "a zero offset must not change one byte of the generated SQL"
    );
    assert!(
        baseline_sql.contains(" LIMIT 20") && !baseline_sql.contains("OFFSET"),
        "the matched bounded top-N renders its LIMIT and no OFFSET: {baseline_sql}"
    );
}

/// Decline (sort key not projected): `ORDER BY` is present but the sort column
/// is not in the projection, so the bounded top-N declines. The PER-SHARD sort
/// keys and LIMIT are still withheld from the common blob (anti-wrong-truncation
/// invariant, decision [4]), but the OUTER wrapper renders a self-contained
/// global `ORDER BY … LIMIT n` (add-topn-pushdown B6): once `ORDER_BY_COLUMN` is
/// advertised Exasol no longer re-applies its own backstop sort/limit, so the
/// adapter reproduces it in the returned SQL.
///
/// The unprojected sort key `L_EXTENDEDPRICE` is APPENDED to the scan as a HIDDEN
/// column (issues #225 / #189) so that outer `ORDER BY` binds against a column the
/// scan actually emits, while the wrapper's visible select list still names only
/// `"L_ORDERKEY"` — the derived projection — keeping the returned arity at 1.
#[test]
fn order_by_present_without_topn_match_withholds_per_shard_limit() {
    // Project only L_ORDERKEY, but ORDER BY L_EXTENDEDPRICE (unprojected).
    let request = serde_json::json!({
        "involvedTables": [{
            "name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
            ],
        }],
        "pushdownRequest": {
            "type": "select",
            "selectList": [
                {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
            ],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                "isAscending": false,
                "nullsLast": true
            }],
            "limit": {"numElements": 20}
        }
    });
    // detect_topn declines the unprojected-key shape.
    assert!(
        detect_topn(
            &request,
            &pd(&request),
            &[ProjectionItem::Column("L_ORDERKEY".into())],
            &lineitem_logical_schema()
        )
        .is_none(),
        "unprojected sort key must decline the top-N path"
    );

    let files = vec![
        ("s3://w/part-0.parquet".to_string(), 1000u64),
        ("s3://w/part-1.parquet".to_string(), 1000u64),
    ];
    let sql = plan_scan_sql(&request, files, 2);

    // The OUTER wrapper renders a self-contained global ORDER BY + LIMIT
    // (reproducing Exasol's former backstop, which no longer runs).
    assert!(
        sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20"#),
        "declined shape must render a self-contained outer ORDER BY … LIMIT: {sql}"
    );
    // The wrapper's VISIBLE select list is the derived projection alone; the
    // appended sort key is emitted by the scan but dropped from the result.
    assert!(
        sql.contains(r#"SELECT "L_ORDERKEY" FROM ("#),
        "wrapper must name only the derived projection, never SELECT *: {sql}"
    );
    assert_eq!(
        outer_select_list(&sql),
        "\"L_ORDERKEY\"",
        "the hidden sort key must not be visible in the outer select list: {sql}"
    );
    assert!(
        emits_clause(&sql).contains("\"L_EXTENDEDPRICE\""),
        "the scan must EMIT the appended hidden sort key: {}",
        emits_clause(&sql)
    );
    // But the PER-SHARD common blob still carries NO sort keys and NO limit:
    // the fan-out stays unbounded and unsorted (anti-wrong-truncation invariant).
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("\"limit\""),
        "declined shape must withhold the per-shard LIMIT from the common blob: {common}"
    );
    assert!(
        !common.contains("order_by"),
        "declined shape must not carry sort keys into the common blob: {common}"
    );
}

/// A LINEITEM row-scan request whose `orderBy` is `order_by` — the shared fixture
/// for the expression-sort-key cases. `select_list` names the VISIBLE columns; any
/// column an `orderBy` expression references but the select list omits must reach
/// the scan as an APPENDED HIDDEN column.
fn lineitem_order_by_request(select_list: &[&str], order_by: Json, limit: Option<u64>) -> Json {
    let type_of = |name: &str| {
        if name == "L_ORDERKEY" {
            serde_json::json!({"type": "decimal", "precision": 20, "scale": 0})
        } else {
            serde_json::json!({"type": "decimal", "precision": 18, "scale": 2})
        }
    };
    let mut pushdown_req = serde_json::json!({
        "type": "select",
        "selectList": select_list
            .iter()
            .map(|name| serde_json::json!({"type": "column", "name": name, "tableName": "LINEITEM"}))
            .collect::<Vec<_>>(),
        "selectListDataTypes": select_list.iter().map(|n| type_of(n)).collect::<Vec<_>>(),
        "orderBy": order_by,
    });
    if let Some(n) = limit {
        pushdown_req["limit"] = serde_json::json!({"numElements": n});
    }
    serde_json::json!({
        "involvedTables": [{
            "name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "dataType": type_of("L_ORDERKEY")},
                {"name": "L_EXTENDEDPRICE", "dataType": type_of("L_EXTENDEDPRICE")},
            ],
        }],
        "pushdownRequest": pushdown_req,
    })
}

/// One `orderBy` element over `expression`, with explicit direction + NULL placement.
fn order_by_element(expression: Json, ascending: bool, nulls_last: bool) -> Json {
    serde_json::json!({
        "type": "order_by_element",
        "expression": expression,
        "isAscending": ascending,
        "nullsLast": nulls_last
    })
}

/// `ABS(<column>)` — the canonical expression sort key from issue #198's repro.
fn abs_of(column: &str) -> Json {
    serde_json::json!({"type": "function_scalar", "name": "ABS", "arguments": [
        {"type": "column", "name": column, "tableName": "LINEITEM"}
    ]})
}

/// A declined `ORDER BY` on an EXPRESSION renders that expression in the Exasol
/// dialect on the outer wrapper and emits the base columns it references as
/// HIDDEN scan columns — the expression-key twin of the bare-column case above
/// (issue #198).
///
/// The rendered name is Exasol's own `ABS`, not DataFusion's `abs`: this wrapper
/// is parsed by Exasol's core engine, so the Exasol dialect reproduces the call
/// Exasol sent (issue #209).
///
/// `("L_EXTENDEDPRICE" + "L_ORDERKEY")` renders identically in both dialects,
/// because `ADD` is an operator wire name the gate's `<NAME>(<args>)` rule
/// cannot derive — not because the two dialects disagree on its shape.
///
/// The referenced column is absent from the select list, so it is APPENDED to
/// the scan's emitted set and dropped again by the wrapper's explicit visible
/// select list, keeping the returned arity at the derived projection's 1.
#[test]
fn declined_order_by_expression_appends_referenced_columns_as_hidden() {
    let request = lineitem_order_by_request(
        &["L_ORDERKEY"],
        serde_json::json!([order_by_element(abs_of("L_EXTENDEDPRICE"), false, true)]),
        Some(20),
    );
    let files = vec![
        ("s3://w/part-0.parquet".to_string(), 1000u64),
        ("s3://w/part-1.parquet".to_string(), 1000u64),
    ];
    let sql = plan_scan_sql(&request, files, 2);

    assert!(
        sql.contains(r#"ORDER BY ABS("L_EXTENDEDPRICE") DESC NULLS LAST LIMIT 20"#),
        "the expression sort key must be rendered on the outer wrapper: {sql}"
    );
    assert_eq!(
        outer_select_list(&sql),
        "\"L_ORDERKEY\"",
        "the hidden referenced column must not be visible in the result: {sql}"
    );
    assert!(
        emits_clause(&sql).contains("\"L_EXTENDEDPRICE\""),
        "the scan must EMIT the referenced column the outer ORDER BY binds against: {}",
        emits_clause(&sql)
    );
    let common = common_arg_literal(&sql);
    assert!(
        common.contains(r#""projection":["L_ORDERKEY","L_EXTENDEDPRICE"]"#),
        "the scan spec must PROJECT the hidden column, not merely declare it in \
         EMITS — the extension runs BEFORE the spec_template literal, or the UDF \
         would never emit the column the EMITS clause promises: {common}"
    );
    assert!(
        !common.contains("order_by") && !common.contains("\"limit\""),
        "the per-shard common blob must stay clean (no sort keys, no limit): {common}"
    );
}

/// Two expression sort keys in ONE clause both render, in order, and their
/// referenced base columns are appended AT MOST ONCE — deduped against each other
/// (`L_EXTENDEDPRICE` is referenced by both keys) and against the existing
/// select-list items (`L_ORDERKEY` is already projected). A repeated EMITS
/// identifier would be a duplicate-column error.
#[test]
fn declined_order_by_two_expression_keys_renders_both_and_leaks_none() {
    let sum_expr = serde_json::json!({"type": "function_scalar", "name": "ADD", "arguments": [
        {"type": "column", "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
        {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"}
    ]});
    let request = lineitem_order_by_request(
        &["L_ORDERKEY"],
        serde_json::json!([
            order_by_element(abs_of("L_EXTENDEDPRICE"), false, true),
            order_by_element(sum_expr, true, false),
        ]),
        None,
    );
    let files = vec![("s3://w/part-0.parquet".to_string(), 1000u64)];
    let sql = plan_scan_sql(&request, files, 1);

    assert!(
        sql.contains(
            r#"ORDER BY ABS("L_EXTENDEDPRICE") DESC NULLS LAST, ("L_EXTENDEDPRICE" + "L_ORDERKEY") ASC NULLS FIRST"#
        ),
        "both expression sort keys must render, in clause order: {sql}"
    );
    let emits = emits_clause(&sql);
    assert_eq!(
        emits.matches("\"L_EXTENDEDPRICE\"").count(),
        1,
        "a column referenced by two keys must be appended exactly once: {emits}"
    );
    assert_eq!(
        emits.matches("\"L_ORDERKEY\"").count(),
        1,
        "an already-projected referenced column must not be appended again: {emits}"
    );
    assert_eq!(
        outer_select_list(&sql),
        "\"L_ORDERKEY\"",
        "no hidden column may leak into the visible select list: {sql}"
    );
}

/// Composition order (#198): an expression sort key whose referenced column IS
/// already projected and which carries a `LIMIT` — the shape that would match the
/// bounded top-N if the bare-column gate were widened. It must NOT: `detect_topn`
/// still declines, the per-shard common blob carries neither sort keys nor a limit,
/// and the query takes the declined wrapper path. The projection is left untouched
/// (nothing to hide), proving the append dedupes against existing select-list items.
#[test]
fn expression_sort_key_declines_bounded_topn_and_takes_declined_path() {
    let request = lineitem_order_by_request(
        &["L_ORDERKEY", "L_EXTENDEDPRICE"],
        serde_json::json!([order_by_element(abs_of("L_EXTENDEDPRICE"), false, true)]),
        Some(20),
    );
    let projected = vec![
        ProjectionItem::Column("L_ORDERKEY".into()),
        ProjectionItem::Column("L_EXTENDEDPRICE".into()),
    ];
    assert!(
        detect_topn(
            &request,
            &pd(&request),
            &projected,
            &lineitem_logical_schema()
        )
        .is_none(),
        "an expression sort key must never match the bounded top-N"
    );

    let files = vec![("s3://w/part-0.parquet".to_string(), 1000u64)];
    let sql = plan_scan_sql(&request, files, 1);

    assert!(
        sql.contains(r#"ORDER BY ABS("L_EXTENDEDPRICE") DESC NULLS LAST LIMIT 20"#),
        "the declined wrapper must render the ordering and the outer LIMIT: {sql}"
    );
    assert_eq!(
        outer_select_list(&sql),
        "\"L_ORDERKEY\", \"L_EXTENDEDPRICE\"",
        "the visible select list stays the derived projection: {sql}"
    );
    assert_eq!(
        emits_clause(&sql).matches("\"L_EXTENDEDPRICE\"").count(),
        1,
        "an already-projected referenced column must not be appended: {}",
        emits_clause(&sql)
    );
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("order_by") && !common.contains("\"limit\""),
        "the bounded top-N declined, so no per-shard sort keys or limit: {common}"
    );
}

/// Every unsupported ordered-query shape declines the top-N path (returns None),
/// while the NQ4 shape matches. Covers: join (multiple involved tables), GROUP
/// BY present, an expression (non-bare-column) sort key, ORDER BY with no LIMIT.
#[test]
fn unsupported_order_by_shape_declines_topn() {
    let projected = vec![
        ProjectionItem::Column("L_ORDERKEY".into()),
        ProjectionItem::Column("L_EXTENDEDPRICE".into()),
    ];

    // Baseline: the well-formed NQ4 shape matches.
    let ok = nq4_request();
    assert_eq!(
        detect_topn(&ok, &pd(&ok), &projected, &lineitem_logical_schema()),
        Some(vec![SortKey {
            column: "L_EXTENDEDPRICE".into(),
            ascending: false,
            nulls_last: true,
        }]),
        "the NQ4 shape must match"
    );

    // Join: two involved tables.
    let mut join = nq4_request();
    let extra_table = serde_json::json!({
        "name": "ORDERS",
        "columns": [{"name": "O_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]
    });
    join.get_mut("involvedTables")
        .and_then(|v| v.as_array_mut())
        .unwrap()
        .push(extra_table);
    assert!(
        detect_topn(&join, &pd(&join), &projected, &lineitem_logical_schema()).is_none(),
        "a multi-table (join) shape must decline"
    );

    // GROUP BY present.
    let mut grouped = nq4_request();
    grouped["pushdownRequest"]["aggregationType"] = serde_json::json!("group_by");
    grouped["pushdownRequest"]["groupBy"] =
        serde_json::json!([{"type": "column", "name": "L_ORDERKEY"}]);
    assert!(
        detect_topn(
            &grouped,
            &pd(&grouped),
            &projected,
            &lineitem_logical_schema()
        )
        .is_none(),
        "a GROUP BY shape must decline"
    );

    // Expression (non-bare-column) sort key.
    let mut expr_key = nq4_request();
    expr_key["pushdownRequest"]["orderBy"] = serde_json::json!([{
        "type": "order_by_element",
        "expression": {"type": "function_scalar", "name": "ABS", "arguments": [
            {"type": "column", "name": "L_EXTENDEDPRICE"}
        ]},
        "isAscending": false,
        "nullsLast": true
    }]);
    assert!(
        detect_topn(
            &expr_key,
            &pd(&expr_key),
            &projected,
            &lineitem_logical_schema()
        )
        .is_none(),
        "an expression sort key must decline (ORDER_BY_EXPRESSION unadvertised)"
    );

    // ORDER BY with no LIMIT: not a bounded top-N.
    let mut no_limit = nq4_request();
    no_limit["pushdownRequest"]
        .as_object_mut()
        .unwrap()
        .remove("limit");
    assert!(
        detect_topn(
            &no_limit,
            &pd(&no_limit),
            &projected,
            &lineitem_logical_schema()
        )
        .is_none(),
        "an ORDER BY without a LIMIT must decline"
    );
}

/// B3b correctness guard: a sort key whose column requires the JSON-fallback
/// VARCHAR cast declines the top-N path, because the per-shard `ORDER BY col`
/// sorts the native value while the emitted `CAST(col AS VARCHAR)` is a JSON
/// string — so Exasol's outer merge would re-rank on the wrong representation.
/// A plain in-range DECIMAL sort key still matches (regression guard), and a
/// sort key absent from the logical schema declines defensively.
#[test]
fn json_fallback_typed_sort_key_declines_topn() {
    let projected = vec![
        ProjectionItem::Column("L_ORDERKEY".into()),
        ProjectionItem::Column("L_EXTENDEDPRICE".into()),
    ];
    let request = nq4_request();

    // Regression: plain in-range DECIMAL sort key (L_EXTENDEDPRICE) matches.
    assert!(
        detect_topn(
            &request,
            &pd(&request),
            &projected,
            &lineitem_logical_schema()
        )
        .is_some(),
        "a plain in-range DECIMAL sort key must still match the top-N shape"
    );

    // The sort key column typed as an OUT-OF-RANGE Decimal128 (emitted as
    // JSON-fallback VARCHAR): the reachable fallback tag from the logical-schema
    // vocabulary (List/Struct/Binary all collapse to `utf8`). Must decline.
    let fallback_schema = vec![
        LogicalField {
            field_id: Some(1),
            name: "L_ORDERKEY".into(),
            arrow_type: "decimal128(20,0)".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "L_EXTENDEDPRICE".into(),
            arrow_type: "decimal128(40,6)".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ];
    assert!(
        crate::types::mapping::needs_json_fallback(&crate::types::mapping::arrow_type_from_tag(
            "decimal128(40,6)"
        )),
        "sanity: the chosen tag must actually be a JSON-fallback type"
    );
    assert!(
        detect_topn(&request, &pd(&request), &projected, &fallback_schema).is_none(),
        "a JSON-fallback-typed sort key must decline the top-N path"
    );

    // The sort key column absent from the logical schema declines defensively.
    let missing_schema = vec![LogicalField {
        field_id: Some(1),
        name: "L_ORDERKEY".into(),
        arrow_type: "decimal128(20,0)".into(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    }];
    assert!(
        detect_topn(&request, &pd(&request), &projected, &missing_schema).is_none(),
        "a sort key absent from the logical schema must decline defensively"
    );
}

/// cap-ext scenario: an ORDER BY the adapter cannot bound as a top-N (here: no
/// LIMIT) is correctness-safe. The bounded top-N declines (no per-shard sort, no
/// per-shard limit in the common blob), but the OUTER wrapper renders a
/// self-contained global `ORDER BY` (no LIMIT) — since once `ORDER_BY_COLUMN` is
/// advertised Exasol no longer re-applies its own backstop sort (add-topn-pushdown
/// B6), the adapter's returned SQL must specify the ordering itself.
#[test]
fn unbounded_order_by_falls_back_correctness_safe() {
    // ORDER BY a projected column but NO LIMIT (unbounded).
    let mut request = nq4_request();
    request["pushdownRequest"]
        .as_object_mut()
        .unwrap()
        .remove("limit");
    let files = vec![("s3://w/part-0.parquet".to_string(), 1000u64)];
    let sql = plan_scan_sql(&request, files, 1);
    assert!(
        sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST"#),
        "unbounded ORDER BY must be rendered self-contained by the adapter: {sql}"
    );
    assert!(
        !sql.contains("LIMIT"),
        "unbounded ORDER BY must not carry any LIMIT: {sql}"
    );
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("order_by") && !common.contains("\"limit\""),
        "per-shard common blob must stay clean (no sort keys, no limit): {common}"
    );
}

/// Row-scan DECLINE with `order_by` but NO `limit` (projected sort column):
/// the outer wrapper renders a self-contained global `ORDER BY` (no LIMIT), and
/// the per-shard common blob stays clean. Proves the decline path no longer
/// withholds the ordering entirely (add-topn-pushdown B6), independent of a
/// LIMIT being present.
#[test]
fn row_scan_decline_order_by_no_limit_wraps_outer_order_by() {
    let request = serde_json::json!({
        "involvedTables": [{
            "name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
            ],
        }],
        "pushdownRequest": {
            "type": "select",
            "selectList": [
                {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
                {"type": "column", "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 18, "scale": 2},
            ],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                "isAscending": false,
                "nullsLast": true
            }]
            // No "limit" key: no LIMIT clause anywhere.
        }
    });
    let files = vec![
        ("s3://w/part-0.parquet".to_string(), 1000u64),
        ("s3://w/part-1.parquet".to_string(), 1000u64),
    ];
    let sql = plan_scan_sql(&request, files, 2);

    assert!(
        sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST"#),
        "no-LIMIT decline must still render a self-contained outer ORDER BY: {sql}"
    );
    assert!(
        !sql.contains("LIMIT"),
        "no LIMIT was requested, so none must be synthesized: {sql}"
    );
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("order_by") && !common.contains("\"limit\""),
        "per-shard common blob must stay clean (no sort keys, no limit): {common}"
    );
}

/// cap-ext scenario (#198): a pushed `ORDER BY` over a SINGLE-GROUP aggregate
/// keeps the request's `LIMIT` — `SELECT COUNT(*) … ORDER BY COUNT(*) LIMIT 0`
/// must return ZERO rows, not the one-row aggregate.
///
/// Driven through the `plan_scan_sql` COMPOSITION mirror, not
/// `build_scan_driving_sql` directly, and that is load-bearing: the leaf
/// renderer takes no `orderBy`, so calling it directly could only hand-feed
/// `request_limit: Some(0)` — the exact value production must derive for
/// itself — and would pass with task 5.1's plumbing absent. The mirror instead
/// reproduces the full dispatch: `order_by_present` is true, `detect_topn` is
/// skipped because `aggregates.is_some()`, and the shared `effective_limit`
/// guard therefore yields `None`. So a rendered `LIMIT 0` can only have arrived
/// via the separate raw-`limit` → `request_limit` channel.
///
/// Both halves are asserted: the outer merge SELECT ends in `LIMIT 0`, AND the
/// per-shard common blob still carries NO `limit`. Together they pin the
/// plumbing, the render site, and the untouched `effective_limit` withholding —
/// a leaked per-shard `LIMIT 0` would zero out each shard's partial instead.
#[test]
fn aggregate_merge_renders_request_limit_zero_through_plan_composition() {
    let request = serde_json::json!({
        "involvedTables": [{
            "name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
            ],
        }],
        "pushdownRequest": {
            "type": "select",
            "aggregationType": "single_group",
            "selectList": [agg_item("COUNT", None, false)],
            "selectListDataTypes": [{"type": "decimal", "precision": 20, "scale": 0}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": agg_item("COUNT", None, false),
                "isAscending": false,
                "nullsLast": true
            }],
            "limit": {"numElements": 0}
        }
    });
    let files = vec![
        ("s3://w/part-0.parquet".to_string(), 1000u64),
        ("s3://w/part-1.parquet".to_string(), 1000u64),
    ];
    let sql = plan_scan_sql(&request, files, 2);

    assert!(
        sql.ends_with(" LIMIT 0"),
        "the outer aggregate merge SELECT must render the request's LIMIT 0: {sql}"
    );
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("\"limit\""),
        "effective_limit stays withheld: the per-shard common blob must carry no \
         limit, or each shard's partial aggregate would be zeroed out: {common}"
    );
}
