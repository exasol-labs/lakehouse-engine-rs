use crate::scan::spec::{LogicalField, ProjectionItem, SortKey};
use serde_json::Value as Json;

use super::support::extract_limit;

// ---------------------------------------------------------------------------
// Ordered top-N pushdown
// ---------------------------------------------------------------------------

/// Parse ONE `orderBy` element into a bare-column [`SortKey`].
///
/// Returns `None` when the element is not a bare `column` node (only
/// `ORDER_BY_COLUMN` is advertised, so Exasol only ever sends bare-column sort
/// keys — anything else is an unexpected shape) or when its `isAscending` /
/// `nullsLast` flags are absent. The column name is uppercased to match the
/// adapter's canonical identifier casing. This is the SINGLE per-element parser
/// shared by [`detect_topn`] (which adds projection + JSON-fallback gates on top)
/// and [`parse_order_by_keys`] (the ungated backstop-restoration parse).
pub(super) fn parse_sort_key_element(element: &Json) -> Option<SortKey> {
    let expr = element.get("expression")?;
    if expr.get("type").and_then(|t| t.as_str()) != Some("column") {
        return None;
    }
    let column = expr
        .get("name")
        .and_then(|n| n.as_str())
        .map(|s| s.to_uppercase())?;
    let ascending = element.get("isAscending").and_then(|b| b.as_bool())?;
    let nulls_last = element.get("nullsLast").and_then(|b| b.as_bool())?;
    Some(SortKey {
        column,
        ascending,
        nulls_last,
    })
}

/// Parse every `orderBy` element into [`SortKey`]s WITHOUT the top-N match gates
/// (projection membership, JSON-fallback type). Used to render the self-contained
/// final `ORDER BY` on the DECLINE / non-matched paths: once `ORDER_BY_COLUMN` is
/// advertised Exasol delegates the ordering and NO LONGER re-applies its own
/// backstop sort, so the adapter must reproduce that global sort in the SQL it
/// returns even for shapes it does not optimize (add-topn-pushdown B6). An element
/// that fails to parse as a bare column is skipped defensively.
pub(super) fn parse_order_by_keys(pushdown_req: &Json) -> Vec<SortKey> {
    pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .map(|elements| elements.iter().filter_map(parse_sort_key_element).collect())
        .unwrap_or_default()
}

/// Detect the ordered-top-N shape and parse its sort keys.
///
/// Returns `Some(keys)` only when EVERY guard holds, so the caller may push the
/// keys as a per-shard bounded sort plus an outer merge `ORDER BY … LIMIT n`:
/// - exactly one involved table (no join),
/// - not a GROUP BY aggregate request (`aggregationType != "group_by"` and no
///   non-empty `groupBy`),
/// - no `having`,
/// - `limit` present with no `offset` (`LIMIT_WITH_OFFSET` is unadvertised, so an
///   offset should never appear — declined defensively if it does),
/// - a non-empty `orderBy` in which EVERY element is a bare `column` node whose
///   uppercased name is one of the projected columns (`ProjectionItem::Column`),
/// - EVERY sort key column resolves to an Arrow type that does NOT require the
///   JSON-fallback VARCHAR cast (`needs_json_fallback` is false for its
///   `LogicalField.arrow_type`).
///
/// The JSON-fallback guard is a correctness requirement, not an optimization: for a
/// fallback-typed column the per-shard scan emits `CAST(col AS VARCHAR)` (a JSON
/// string) but its `ORDER BY col` sorts by the column's REAL native value (the cast
/// lives only in the SELECT list, not the FROM-clause row source the ORDER BY binds
/// against). Exasol's outer merge sees ONLY the emitted JSON string, so it re-ranks
/// lexicographically — a representation the per-shard sort never used. Per-shard and
/// merge would disagree on ranking and silently corrupt the global top-N. Declining
/// falls back to the safe raw-scan path (Exasol re-applies ORDER BY/LIMIT).
/// (The tag vocabulary collapses List/Struct/Binary/etc to `utf8`, so the reachable
/// fallback tag today is an out-of-range `decimal128(p>36,…)`; the guard is the
/// correct seam regardless and stays correct if the tag vocabulary is enriched.)
///
/// A sort key column absent from `logical_schema` declines defensively (rather than
/// assuming a safe type) — it should never happen, since the key is already required
/// to be a projected column.
///
/// Any deviation returns `None` — the caller then withholds the limit (never a
/// bare per-shard/outer LIMIT ahead of an ordering the adapter did not render) and
/// falls back to the pre-existing plan, leaving row selection to Exasol.
///
/// Only ever called on the pure row-scan path (no aggregates); the GROUP BY and
/// aggregate guards below make it self-contained and independently testable.
pub(super) fn detect_topn(
    request: &Json,
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    logical_schema: &[LogicalField],
) -> Option<Vec<SortKey>> {
    // A top-N needs a bound. Limit must be present with no offset.
    extract_limit(pushdown_req)?;
    if pushdown_req
        .get("limit")
        .and_then(|l| l.get("offset"))
        .is_some()
    {
        return None;
    }

    // Reject GROUP BY / grouped-aggregate shapes: ordered top-N over aggregated or
    // grouped results is out of scope (mission non-goal).
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        return None;
    }
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    // Reject HAVING (only meaningful with grouping; a defensive belt with the above).
    if pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .is_some()
    {
        return None;
    }

    // Single involved table only — a multi-table (join) shape declines.
    let table_count = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .map(|t| t.len())
        .unwrap_or(0);
    if table_count != 1 {
        return None;
    }

    // Parse each sort key: it must be a bare `column` node that is also projected.
    let elements = pushdown_req.get("orderBy").and_then(|v| v.as_array())?;
    if elements.is_empty() {
        return None;
    }
    let mut keys = Vec::with_capacity(elements.len());
    for element in elements {
        // Bare-column shape + direction/NULL flags (shared parser); a missing flag
        // or a non-column node is an unexpected shape → decline.
        let key = parse_sort_key_element(element)?;
        // The sort key must be one of the projected columns (per the plan's scope:
        // sort on already-emitted columns, no extra machinery). An expression
        // projection (`ProjectionItem::Expr`) is never a bare-column sort target.
        let projected = proj_cols
            .iter()
            .any(|p| matches!(p, ProjectionItem::Column(c) if *c == key.column));
        if !projected {
            return None;
        }
        // Decline any sort key whose column requires the JSON-fallback VARCHAR cast:
        // its emitted representation (a JSON string) would not match the native value
        // the per-shard ORDER BY sorts by, so the outer merge would re-rank on the
        // wrong representation and corrupt the global top-N. Resolve the column's
        // Arrow type from its logical-schema tag (the only type info available at plan
        // time). A column absent from the logical schema declines defensively.
        let arrow_type = logical_schema
            .iter()
            .find(|f| f.name.to_uppercase() == key.column)
            .map(|f| crate::types::mapping::arrow_type_from_tag(&f.arrow_type))?;
        if crate::types::mapping::needs_json_fallback(&arrow_type) {
            return None;
        }
        keys.push(key);
    }
    Some(keys)
}

#[cfg(test)]
mod tests {
    use super::super::support::{
        DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, aggregate_exasol_types, build_scan_driving_sql,
        extract_all_column_types, extract_projection, order_by_present, shard_count,
    };
    use super::super::test_support::*;
    use super::super::{detect_aggregates, ordinary_plans, validate_agg_col_types};
    use super::*;
    use crate::scan::spec::{CommonScanSpec, FileEntry, ScanSpec, render_order_by_clause};
    use vs_expression::render_df_filter_safe;

    // -----------------------------------------------------------------------
    // Ordered top-N pushdown (B3)
    // -----------------------------------------------------------------------

    /// Reproduce `handle_pushdown`'s SYNCHRONOUS row-scan decision path (everything
    /// after `resolve_file_list`) so tests exercise the real `detect_topn`,
    /// `effective_limit` withholding glue, and `build_scan_driving_sql` — the exact
    /// composition production runs, minus the network file resolution.
    fn plan_scan_sql(request: &Json, files: Vec<(String, u64)>, cluster_nodes: usize) -> String {
        let pushdown_req = request
            .get("pushdownRequest")
            .cloned()
            .unwrap_or(Json::Null);
        let (proj_cols, proj_types) = extract_projection(request, &pushdown_req).unwrap();
        let filter = pushdown_req
            .get("filter")
            .filter(|f| !f.is_null())
            .and_then(render_df_filter_safe);
        let limit = extract_limit(&pushdown_req);
        let has_order_by = order_by_present(&pushdown_req);
        let col_types = extract_all_column_types(request);

        let items = detect_aggregates(&pushdown_req)
            .filter(|it| validate_agg_col_types(&ordinary_plans(it), &col_types));
        let aggregates = items.map(|it| ordinary_plans(&it));
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

        let spec_template = ScanSpec {
            common: CommonScanSpec {
                table_root: String::new(),
                projection: proj_cols.clone(),
                filter,
                limit: effective_limit,
                order_by,
                aggregates,
                group_keys: None,
                distinct: false,
                emit_exa_types: proj_types.clone(),
                logical_schema: Vec::new(),
                name_mapping: Vec::new(),
                join: None,
                storage: sample_storage(),
                df_target_partitions: 1,
                df_batch_size: 8192,
                df_threads_per_udf: 1,
                memory_pool_fraction: 0.6,
                instance_overhead_mb: 200,
                s3_max_connections: 8,
            },
            files: vec![],
        };
        let files: Vec<FileEntry> = files.into_iter().map(FileEntry::from).collect();
        let g = shard_count(cluster_nodes, 1, files.len());
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let aggregate_types = aggregate_exasol_types(&pushdown_req);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &proj_cols,
            &proj_types,
            effective_limit,
            &col_types,
            &aggregate_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        // Mirror handle_pushdown's row-scan DECLINE wrapping (add-topn-pushdown B6).
        let declined_order_by = has_order_by
            && spec_template.common.order_by.is_empty()
            && spec_template.common.aggregates.is_none();
        if declined_order_by {
            let keys = parse_order_by_keys(&pushdown_req);
            if keys.is_empty() {
                sql
            } else {
                let mut wrapped = format!(
                    "SELECT * FROM ({sql}) ORDER BY {}",
                    render_order_by_clause(&keys)
                );
                if let Some(n) = limit {
                    wrapped.push_str(&format!(" LIMIT {n}"));
                }
                wrapped
            }
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
                field_id: 1,
                name: "L_ORDERKEY".into(),
                arrow_type: "decimal128(20,0)".into(),
                nullable: true,
                initial_default: None,
            },
            LogicalField {
                field_id: 2,
                name: "L_EXTENDEDPRICE".into(),
                arrow_type: "decimal128(18,2)".into(),
                nullable: true,
                initial_default: None,
            },
        ]
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

    /// Decline (sort key not projected): `ORDER BY` is present but the sort column
    /// is not in the projection, so the bounded top-N declines. The PER-SHARD sort
    /// keys and LIMIT are still withheld from the common blob (anti-wrong-truncation
    /// invariant, decision [4]), but the OUTER wrapper now renders a self-contained
    /// global `ORDER BY … LIMIT n` (add-topn-pushdown B6): once `ORDER_BY_COLUMN` is
    /// advertised Exasol no longer re-applies its own backstop sort/limit, so the
    /// adapter reproduces it in the returned SQL.
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
                field_id: 1,
                name: "L_ORDERKEY".into(),
                arrow_type: "decimal128(20,0)".into(),
                nullable: true,
                initial_default: None,
            },
            LogicalField {
                field_id: 2,
                name: "L_EXTENDEDPRICE".into(),
                arrow_type: "decimal128(40,6)".into(),
                nullable: true,
                initial_default: None,
            },
        ];
        assert!(
            crate::types::mapping::needs_json_fallback(
                &crate::types::mapping::arrow_type_from_tag("decimal128(40,6)")
            ),
            "sanity: the chosen tag must actually be a JSON-fallback type"
        );
        assert!(
            detect_topn(&request, &pd(&request), &projected, &fallback_schema).is_none(),
            "a JSON-fallback-typed sort key must decline the top-N path"
        );

        // The sort key column absent from the logical schema declines defensively.
        let missing_schema = vec![LogicalField {
            field_id: 1,
            name: "L_ORDERKEY".into(),
            arrow_type: "decimal128(20,0)".into(),
            nullable: true,
            initial_default: None,
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
}
