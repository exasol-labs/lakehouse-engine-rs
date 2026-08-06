use crate::scan::spec::ProjectionItem;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use std::collections::HashMap;
use vs_expression::{render_df_filter_exasol_safe, render_expression_exasol_safe};

use super::super::support::{
    datafusion_renderable, project_columns, quote_ident, type_accepted_rewrite, walk_column_nodes,
};
use super::planning::{DetectedJoin, involved_table_columns};

/// The SOLE producer of a join's column-type union: `join.tables[0]`'s
/// [`involved_table_columns`] extended with `join.tables[1]`'s. Broadcast is a
/// two-table optimization, so `join.tables[0]`/`[1]` are the two involved tables.
///
/// Every consumer that needs "the type universe a broadcast-join filter or
/// projection may be screened against" MUST call this rather than re-deriving the
/// union itself — [`extract_join_projection`] and `render_broadcast_join`'s
/// `classify_where_filter` call both do. The caller must have already passed the
/// [`disjoint_schema_guard`](super::planning::disjoint_schema_guard) so the union
/// carries no name collision — a bare column name resolves to exactly one Exasol
/// type only once that guard has passed.
pub(super) fn join_col_types(request: &Json, join: &DetectedJoin) -> Vec<(String, String)> {
    let mut combined = involved_table_columns(request, &join.tables[0].table_name);
    combined.extend(involved_table_columns(request, &join.tables[1].table_name));
    combined
}

pub(super) fn extract_join_projection(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
) -> Result<(Vec<ProjectionItem>, Vec<String>, bool), UdfError> {
    project_columns(pushdown_req, join_col_types(request, join))
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
/// THREE consumers receive DIFFERENT trees built from this function's output,
/// deliberately:
/// (a) that side's `resolve_file_list` for Iceberg manifest pruning is given the
/// RAW filter, unscreened, so every side-local conjunct prunes manifests even when
/// the DataFusion dialect cannot render it — screening here would silently open
/// more files while still returning correct rows;
/// (b) that side's fan-out `ScanSpec.filter` is given a tree first screened by
/// [`renderable_only`], then screened AND REWRITTEN per side by
/// [`type_screened_leg_filter`], so the leg receives only conjuncts that are both
/// syntactically renderable and type-correct for that side's own columns; and
/// (c) the outer wrapper's residual `WHERE` receives the RAW conjuncts
/// [`type_screened_leg_filter`] hands back declined, because the wrapper renders in
/// the Exasol dialect and a DataFusion-rewritten tree is the wrong input there.
/// Cross-table conjuncts and OR-spanning conjuncts are withheld from (a) and (b)
/// and applied only by the outer wrapper's WHERE, alongside the type-declined half.
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

/// Split ONE side's side-local conjuncts into the REWRITTEN set its fan-out leg may
/// render and the RAW set the outer wrapper must apply: returns
/// `(leg_filter, type_declined)`, a partition of `side_local`'s top-level conjuncts
/// that is total (every conjunct lands in exactly one half) and type-correct for that
/// one side.
///
/// The N-scan analog of the broadcast surface's
/// [`classify_where_filter`](super::super::support::classify_where_filter), and
/// deliberately NOT a call to it: that function owns a WHOLE-filter classification
/// against ONE type universe, which neither half of this surface's situation matches.
/// Both surfaces do share the acceptance predicate underneath, and ask
/// [`type_accepted_rewrite`] for it rather than each encoding it.
///
/// PER-SIDE, POST-ATTRIBUTION. The N-scan path has no disjoint-column-name
/// precondition (the broadcast path's `disjoint_schema_guard` is what earns the
/// broadcast surface its single union universe), so a bare column name here can
/// resolve to a DIFFERENT Exasol type on each side. The only universe that answers
/// "will DataFusion accept this conjunct in THIS leg" is the owning side's own
/// `col_types` — knowable only after [`side_local_filter`] has attributed the
/// conjunct, hence a screen that runs after attribution rather than over the request's
/// whole filter.
///
/// PER-CONJUNCT, NOT PER-TREE. One type-declining conjunct must not forfeit its
/// side's other pushable conjuncts; the outer wrapper's WHERE absorbs exactly the
/// rejected ones. The single-table WHERE surface declines whole-filter only because it
/// has no partition to absorb one conjunct into.
///
/// SCREENED ON THE REWRITTEN TREE. The leg renders what this function returns — the
/// REWRITTEN tree — so renderability must be established on that tree, not on the raw
/// one. [`type_accepted_rewrite`] is what establishes it, for the per-conjunct screen
/// and the re-formed tree alike, and the broadcast site inherits the same guarantee
/// from the same call.
///
/// FAILS CLOSED TOWARDS THE RESIDUAL. Should the re-formed accepted tree not itself
/// survive [`type_accepted_rewrite`], the WHOLE side-local set becomes residual. A
/// conjunct applied nowhere returns wrong rows; a conjunct applied in the outer
/// wrapper instead of a leg is merely slower.
///
/// The declined half is returned RAW because the outer wrapper renders it in the
/// EXASOL dialect: the rewrites synthesize DataFusion-dialect nodes, so a rewritten
/// tree is the wrong input there — the same reason `classify_where_filter` returns its
/// declined half un-rewritten.
pub(super) fn type_screened_leg_filter(
    side_local: &Json,
    col_types: &[(String, String)],
) -> (Option<Json>, Option<Json>) {
    let accepts = |c: &Json| type_accepted_rewrite(c, col_types).is_some();
    let declined = partition_conjuncts(side_local, |c| !accepts(c));
    match partition_conjuncts(side_local, accepts) {
        None => (None, declined),
        Some(accepted) => match type_accepted_rewrite(&accepted, col_types) {
            Some(rewritten) => (Some(rewritten), declined),
            None => (None, Some(side_local.clone())),
        },
    }
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
#[path = "rendering_tests.rs"]
mod tests;
