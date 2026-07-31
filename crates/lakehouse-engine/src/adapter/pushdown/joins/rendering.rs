use crate::scan::spec::ProjectionItem;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use std::collections::HashMap;
use vs_expression::{render_df_filter_exasol_safe, render_expression_exasol_safe};

use super::super::support::{
    datafusion_renderable, project_columns, quote_ident, walk_column_nodes,
};
use super::planning::{DetectedJoin, involved_table_columns};

/// The cross-table projection and Exasol EMITS types for a broadcast join.
///
/// Reuses [`project_columns`] against the disjoint union of both involved tables'
/// columns, so a projected column spanning either side is typed from whichever
/// side owns it. The caller must have already passed the [`disjoint_schema_guard`]
/// so the union carries no name collision. Broadcast is a two-table optimization,
/// so `join.tables[0]`/`[1]` are the two involved tables.
///
/// The third element is [`project_columns`]'s widening signal, forwarded verbatim:
/// `true` means the derived projection is the full two-table base row, not one item
/// per select-list item (#196).
pub(super) fn extract_join_projection(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
) -> Result<(Vec<ProjectionItem>, Vec<String>, bool), UdfError> {
    let mut combined = involved_table_columns(request, &join.tables[0].table_name);
    combined.extend(involved_table_columns(request, &join.tables[1].table_name));
    project_columns(pushdown_req, combined)
}

/// Render one projection item as an outer-query SELECT expression: a bare column is
/// double-quoted, an already-rendered scalar expression is spliced verbatim.
pub(super) fn projection_item_select_sql(item: &ProjectionItem) -> String {
    match item {
        ProjectionItem::Column(name) => quote_ident(name),
        ProjectionItem::Expr { expr } => expr.clone(),
    }
}

/// Deep-clone an expression node, tagging every `column` node with the subquery
/// alias its `tableName` maps to (`tableAlias`), so `vs-expression` renders it as a
/// table-qualified reference (`"ALIAS"."NAME"`).
///
/// This is the seam that makes the two-scan wrapper correct regardless of whether
/// the two joined tables share a column name: bare-name rendering (the broadcast
/// path) is ambiguous on a collision, but a table-qualified reference resolved
/// against each side's OWN fan-out subquery never is. A `column` whose `tableName`
/// is not in `alias_of` is left unqualified (it belongs to neither joined table —
/// which cannot happen for a well-formed two-table request).
fn annotate_columns_with_alias(expr: &Json, alias_of: &HashMap<String, String>) -> Json {
    match expr {
        Json::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len() + 1);
            for (key, value) in map {
                out.insert(key.clone(), annotate_columns_with_alias(value, alias_of));
            }
            if map.get("type").and_then(|t| t.as_str()) == Some("column")
                && let Some(alias) = map
                    .get("tableName")
                    .and_then(|t| t.as_str())
                    .and_then(|t| alias_of.get(&t.to_ascii_uppercase()))
            {
                out.insert("tableAlias".to_string(), Json::String(alias.clone()));
            }
            Json::Object(out)
        }
        Json::Array(items) => Json::Array(
            items
                .iter()
                .map(|item| annotate_columns_with_alias(item, alias_of))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Render an expression node to table-qualified **Exasol** SQL for the two-scan
/// wrapper: annotate each `column` with its side alias, then reuse the
/// `vs-expression` translator via its Exasol-dialect entry point. `None` when the
/// node cannot be rendered.
///
/// One recursive translator covers every node shape the qualified N-scan wrapper's
/// select list needs — columns, literals, scalar expressions, a top-level
/// `function_aggregate`, AND a `function_aggregate` nested inside a scalar function
/// — with no separate select-list-specific renderer. The translator splices an
/// Exasol aggregate `name` verbatim (Exasol pushed it, so it is a valid Exasol
/// aggregate — `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, the STDDEV/VARIANCE family),
/// renders each argument by recursion (table-qualifying any column argument via its
/// `tableAlias`), handles `COUNT(*)`, and honors `DISTINCT`. This is byte-compatible
/// with the former top-level `render_aggregate_qualified` (single-arg aggregate →
/// `NAME(<arg>)`, `COUNT(*)` → `COUNT(*)`), and additionally renders a scalar
/// expression that wraps aggregates (e.g. `ROUND(100.0 * SUM(CASE …) / COUNT(*),
/// 2)`) instead of declining.
///
/// This whole module builds outer-wrapper SQL that Exasol's own core engine
/// parses directly, so CAST targets must use Exasol syntax (length-qualified
/// `VARCHAR(n)`), unlike the DataFusion-side `ScanSpec` renders elsewhere in the
/// join-rendering path (`render_broadcast_join`'s `render_expression_safe` call)
/// which stay on the bare-`VARCHAR` DataFusion dialect.
pub(super) fn render_expression_qualified(
    expr: &Json,
    alias_of: &HashMap<String, String>,
) -> Option<String> {
    render_expression_exasol_safe(&annotate_columns_with_alias(expr, alias_of))
}

/// Render a WHERE filter to a table-qualified **Exasol** boolean expression for
/// the two-scan wrapper. `None` when the filter is absent-shaped, trivially true,
/// or unrenderable — mirroring the single-table `render_df_filter_safe` contract.
/// A `None` here is never Exasol's problem to catch: the caller must itself
/// self-apply a declined filter (e.g. as an outer WHERE) rather than omit it
/// (`pushdown`'s module header). Uses
/// the Exasol-dialect entry point because the wrapper WHERE is parsed by Exasol's
/// core engine (length-qualified CAST targets).
pub(super) fn render_df_filter_qualified(
    filter: &Json,
    alias_of: &HashMap<String, String>,
) -> Option<String> {
    render_df_filter_exasol_safe(&annotate_columns_with_alias(filter, alias_of))
}

/// Walk an expression tree, returning every `column` node's owning side: the set of
/// UPPERCASE `tableName`s seen, whether any `column` carried no `tableName`
/// (`has_untagged`), and whether any `column` node was seen at all (`any_column`).
///
/// `tableName` is the SAME attribution signal [`annotate_columns_with_alias`] uses,
/// so conjunct-to-side attribution is by table identity — never by column name,
/// which keeps the shared-column-name case (both tables carry an `ID`) correct.
pub(super) fn column_tables(expr: &Json) -> (std::collections::HashSet<String>, bool, bool) {
    let mut tables = std::collections::HashSet::new();
    let mut has_untagged = false;
    let mut any_column = false;
    walk_column_nodes(expr, &mut |map| {
        any_column = true;
        match map.get("tableName").and_then(|t| t.as_str()) {
            Some(tn) => {
                tables.insert(tn.to_ascii_uppercase());
            }
            None => has_untagged = true,
        }
    });
    (tables, has_untagged, any_column)
}

/// The single side a conjunct is local to — `Some(UPPERCASE table name)` iff every
/// `column` node it references is tagged with that ONE `tableName`. `None` when the
/// conjunct spans two tables, carries an untagged column, or references no column at
/// all (a bare literal). Such a conjunct is withheld from BOTH sides' pruning; only
/// the outer wrapper's WHERE (which renders the full predicate) applies it.
///
/// Sound for an inner equi-join: a conjunct over one side alone is a necessary
/// condition for that side's rows to survive the join, so using it to prune that
/// side can never drop a row the join would have kept.
fn conjunct_single_side(conjunct: &Json) -> Option<String> {
    let (tables, has_untagged, any_column) = column_tables(conjunct);
    if has_untagged || !any_column || tables.len() != 1 {
        return None;
    }
    tables.into_iter().next()
}

/// Flatten a top-level `predicate_and` chain into its individual conjuncts,
/// recursing through nested `predicate_and` nodes (AND is associative). A non-AND
/// node (including a top-level `predicate_or`) is a single opaque conjunct — an OR
/// is never split, so an OR spanning both tables stays withheld from both sides.
fn flatten_conjuncts<'a>(filter: &'a Json, out: &mut Vec<&'a Json>) {
    if filter.get("type").and_then(|t| t.as_str()) == Some("predicate_and")
        && let Some(exprs) = filter.get("expressions").and_then(|e| e.as_array())
    {
        for expr in exprs {
            flatten_conjuncts(expr, out);
        }
        return;
    }
    out.push(filter);
}

/// Keep the top-level conjuncts of `filter` that `keep` selects and re-form them
/// into one sub-predicate: `None` when none are kept, the bare conjunct when exactly
/// one is, else a `predicate_and` over all kept conjuncts.
///
/// The shared shape of the two complementary screen pairs over one filter — only
/// the `keep` predicate differs: [`side_local_filter`] (conjuncts local to one
/// side) against [`cross_side_residual_filter`] (the cross-side complement), and
/// [`renderable_only`] against [`declined_only`].
fn partition_conjuncts(filter: &Json, keep: impl Fn(&Json) -> bool) -> Option<Json> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(filter, &mut conjuncts);
    let mut kept: Vec<Json> = conjuncts
        .into_iter()
        .filter(|&c| keep(c))
        .cloned()
        .collect();
    match kept.len() {
        0 => None,
        1 => kept.pop(),
        _ => Some(serde_json::json!({
            "type": "predicate_and",
            "expressions": kept,
        })),
    }
}

/// The side-local sub-predicate of `filter` for `table_name`: the AND of exactly
/// those top-level conjuncts every column of which is attributed to `table_name`.
/// `None` when no conjunct is side-local to it. Attribution by `tableName` alone —
/// this makes NO renderability decision, and each consumer screens (or does not
/// screen) its own input before calling.
///
/// The two consumers therefore receive DIFFERENT trees, deliberately:
/// (a) that side's `resolve_file_list` for Iceberg manifest pruning is given the
/// RAW filter, so every side-local conjunct prunes manifests even when the
/// DataFusion dialect cannot render it — screening here would silently open more
/// files while still returning correct rows; and (b) that side's fan-out
/// `ScanSpec.filter` is given a tree already screened by [`renderable_only`], so
/// the conjuncts it yields are all renderable and the leg's own render cannot
/// decline. Cross-table conjuncts and OR-spanning conjuncts are withheld from both
/// and applied only by the outer wrapper's WHERE.
pub(super) fn side_local_filter(filter: &Json, table_name: &str) -> Option<Json> {
    let target = table_name.to_ascii_uppercase();
    partition_conjuncts(filter, |c| {
        conjunct_single_side(c).as_deref() == Some(target.as_str())
    })
}

/// The cross-side residual sub-predicate of `filter`: the AND of exactly those
/// top-level conjuncts that are NOT side-local to a single table — i.e. cross-table,
/// OR-spanning, untagged, or column-free conjuncts (`conjunct_single_side` is
/// `None`). `None` when every conjunct is side-local.
///
/// The complement it forms is over WHATEVER TREE IT IS GIVEN, not over the request's
/// raw filter: it is the exact set-complement of the per-side [`side_local_filter`]
/// slices of that same tree, and nothing more. On the render path it is given the
/// [`renderable_only`] half, so the outer wrapper's WHERE additionally carries
/// [`declined_only`] — the total partition of the request's filter is therefore
/// `renderable_only`/`declined_only` composed with these two, and it is that
/// composition, not this function alone, that leaves no conjunct dropped or
/// double-applied.
pub(super) fn cross_side_residual_filter(filter: &Json) -> Option<Json> {
    partition_conjuncts(filter, |c| conjunct_single_side(c).is_none())
}

/// The DataFusion-RENDERABLE half of `filter`'s top-level conjuncts, and
/// [`declined_only`] its exact complement — the sole renderability screen on the
/// N-scan render path, applied at [`super::sql_builders::build_n_scan_join_sql`]'s
/// two render sites and NOWHERE else.
///
/// It sits at the render sites rather than inside [`side_local_filter`] because
/// that function has a second consumer that must NOT be screened: `plan_join`
/// passes its result to Iceberg manifest pruning, where dropping a declined
/// conjunct would silently open more files while still returning correct rows —
/// a regression no test could catch. Only the leg's `ScanSpec.filter` is
/// rendered, so only it needs screening; a conjunct this rejects is carried by
/// the outer wrapper's WHERE in the Exasol dialect instead of being omitted.
pub(super) fn renderable_only(filter: &Json) -> Option<Json> {
    partition_conjuncts(filter, datafusion_renderable)
}

/// The DataFusion-DECLINED half of `filter`'s top-level conjuncts — the exact
/// complement of [`renderable_only`], and the set the outer wrapper's WHERE must
/// carry because no leg can apply it.
pub(super) fn declined_only(filter: &Json) -> Option<Json> {
    partition_conjuncts(filter, |c| !datafusion_renderable(c))
}

/// AND two optional sub-predicates into one: the `predicate_and` of both when
/// both are present, the present one alone when only one is, `None` when neither
/// is.
///
/// Callers must pass DISJOINT conjunct sets — this de-duplicates nothing, so
/// overlapping inputs would double-apply a predicate.
pub(super) fn conjoin_filters(left: Option<Json>, right: Option<Json>) -> Option<Json> {
    match (left, right) {
        (Some(l), Some(r)) => Some(serde_json::json!({
            "type": "predicate_and",
            "expressions": [l, r],
        })),
        (l, r) => l.or(r),
    }
}

/// Record the UPPERCASE name of every `column` node in `expr` attributed (by
/// `tableName`, case-insensitive) to `table_name`, recursively.
fn collect_side_column_names(
    expr: &Json,
    table_name: &str,
    out: &mut std::collections::HashSet<String>,
) {
    walk_column_nodes(expr, &mut |map| {
        let tn = map.get("tableName").and_then(|t| t.as_str());
        let name = map.get("name").and_then(|n| n.as_str());
        if let (Some(tn), Some(name)) = (tn, name)
            && tn.eq_ignore_ascii_case(table_name)
        {
            out.insert(name.to_ascii_uppercase());
        }
    });
}

/// Visit every clause of `pushdown_req` whose rendered SQL can name a source column:
/// `selectList`, a non-null `filter`, `groupBy`, `orderBy`, then a non-null `having`.
///
/// The single owner of *which* clauses those are, so adding or removing one is a
/// one-function edit rather than a two-function edit kept in sync by hand. It owns the
/// clause set and nothing else: the per-node collector is a parameter because the two
/// callers must stay divergent in ways this walk has no business reconciling. They
/// fold case differently — [`referenced_side_columns`] collects through
/// `collect_side_column_names`' ASCII-only `to_ascii_uppercase`,
/// `referenced_column_projection` through `collect_all_column_names`' Unicode
/// `to_uppercase`, a disagreement `walk_column_nodes`' doc comment and
/// `vs-adapter/pushdown-module-structure`'s "One blind traversal primitive backs every
/// column-collecting walk" scenario both forbid unifying — and they fall back
/// differently when the narrowing selects nothing.
///
/// [`referenced_side_columns`] deliberately keeps its own absent/empty-`selectList`
/// short-circuit BEFORE calling this, so `selectList` is named twice by design. That
/// guard MUST NOT be folded in here: it is a fallback policy, not part of the clause
/// set, and folding it in would hand `referenced_column_projection` a short-circuit
/// that `vs-adapter/pushdown-joins-module-structure`'s "One clause walk feeds both
/// wrapper column-narrowing routines" scenario forbids it — that path must keep
/// narrowing through the remaining clauses when the select list is absent or empty.
pub(super) fn referenced_clause_values(pushdown_req: &Json, mut visit: impl FnMut(&Json)) {
    if let Some(list) = pushdown_req.get("selectList") {
        visit(list);
    }
    if let Some(f) = pushdown_req.get("filter").filter(|f| !f.is_null()) {
        visit(f);
    }
    for key in ["groupBy", "orderBy"] {
        if let Some(v) = pushdown_req.get(key) {
            visit(v);
        }
    }
    if let Some(h) = pushdown_req.get("having").filter(|h| !h.is_null()) {
        visit(h);
    }
}

/// The subset of `full_cols` this side actually contributes to the outer two-scan
/// wrapper — dropping columns the wrapper never references, so each fan-out leg
/// ships narrow rows instead of the table's full column set.
///
/// The kept set is every column of this side referenced by any clause the wrapper
/// renders: the SELECT list, the join condition, the WHERE (the FULL predicate —
/// the outer wrapper renders all of it, so a side-local *or* cross-table filter
/// column must survive), GROUP BY, HAVING, and ORDER BY. The request's share of that
/// set comes from [`referenced_clause_values`]; the join condition is collected
/// separately because it is not a clause of the request. Order and Exasol types are
/// preserved from `full_cols`.
///
/// Two total-safety fallbacks keep the wrapper buildable: an absent/empty SELECT
/// list means `SELECT *` over both fan-outs, so every column is kept; and an
/// (unreachable) empty result keeps `full_cols` rather than emit a zero-column leg.
///
/// The `names.contains(name)` narrowing below is a CROSS-FOLD string match: `full_cols`
/// arrives from [`involved_table_columns`] folded by `support::column_types`' Unicode
/// `to_uppercase`, while `names` is folded by `collect_side_column_names`' ASCII-only
/// `to_ascii_uppercase`. The two agree only by premise — `resolve_table_schema`
/// Unicode-uppercases every name it declares, so no LOWERCASE name reaches either side
/// (guarded by the E2E test `non_ascii_table_and_column_stay_queryable`). Non-ASCII
/// letters can still reach both sides (e.g. `über` uppercases to `ÜBER`, not to an
/// ASCII form) — the two folds still agree there because `to_ascii_uppercase` only
/// touches ASCII `a`-`z`, none of which remain once a name is already
/// Unicode-uppercased. Repair any divergence at that premise, never by unifying the
/// two folds. If the premise ever weakens, `full_cols` would hold `STRASSE` where
/// `names` holds `STRAßE`; this filter would then drop a column the outer wrapper
/// still references, and the empty-result fallback named above rescues only a
/// *fully* empty narrowing — a partial mismatch narrows a referenced column away
/// instead. That is a dropped column, not necessarily a silent one: if the outer
/// wrapper's rendered SQL still references it elsewhere, Exasol surfaces a
/// column-not-found error rather than a silently wrong result.
pub(super) fn referenced_side_columns(
    pushdown_req: &Json,
    condition: &Json,
    table_name: &str,
    full_cols: &[(String, String)],
) -> Vec<(String, String)> {
    // Absent/empty select list ⇒ the wrapper projects every column (SELECT *).
    if !matches!(pushdown_req.get("selectList"), Some(Json::Array(list)) if !list.is_empty()) {
        return full_cols.to_vec();
    }
    let mut names = std::collections::HashSet::new();
    collect_side_column_names(condition, table_name, &mut names);
    referenced_clause_values(pushdown_req, |v| {
        collect_side_column_names(v, table_name, &mut names)
    });
    let narrowed: Vec<(String, String)> = full_cols
        .iter()
        .filter(|(name, _)| names.contains(name))
        .cloned()
        .collect();
    if narrowed.is_empty() {
        full_cols.to_vec()
    } else {
        narrowed
    }
}

#[cfg(test)]
mod tests {
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
        let narrowed =
            referenced_side_columns(&serde_json::json!({}), &condition, "CUSTOMER", &full);
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
            !common.contains(r#"\"O\".\"O_ORDERDATE\""#)
                && !common.contains(r#""O"."O_ORDERDATE""#),
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

    /// The broadcast path renders `rendered.filter` exactly as before, PRESERVING
    /// Exasol's native `tableAlias` qualifier (the in-UDF `build_join_sql` join
    /// resolves it) — the two-scan fan-out's bare-alias stripping (used only by
    /// [`build_side_fan_out_sql`]) must NOT leak into, nor alter, the broadcast
    /// rendering.
    #[test]
    fn render_broadcast_join_preserves_native_table_alias_unchanged() {
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
            filter.contains(r#""O"."O_ORDERDATE""#),
            "broadcast rendering must preserve Exasol's native tableAlias (unchanged): {filter}"
        );
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
}
