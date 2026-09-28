use crate::scan::spec::ProjectionItem;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{render_df_filter_exasol_safe, render_expression_exasol_safe};

use super::super::support::{
    datafusion_renderable, project_columns, quote_ident, type_accepted_rewrite, walk_column_nodes,
};
use super::attribution::{ColumnLeg, JoinLegs, UnattributableColumn};
use super::planning::{DetectedJoin, involved_table_columns};

/// The sole producer of a broadcast join's column-type union (broadcast is two-table only).
/// Callers must have passed [`disjoint_schema_guard`](super::planning::disjoint_schema_guard)
/// so each bare name maps to exactly one type.
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

pub(super) fn projection_item_select_sql(item: &ProjectionItem) -> String {
    match item {
        ProjectionItem::Column(name) => quote_ident(name),
        ProjectionItem::Expr { expr } => expr.clone(),
    }
}

/// Stamps each `column` with its leg's subquery alias, then renders in the Exasol dialect,
/// since the N-scan wrapper is parsed by Exasol (length-qualified `VARCHAR(n)` CAST targets,
/// unlike the DataFusion-dialect broadcast render). `Err` on an unattributable reference,
/// which the caller turns into a hard decline.
pub(super) fn render_expression_qualified(
    expr: &Json,
    legs: &JoinLegs,
) -> Result<Option<String>, UnattributableColumn> {
    Ok(render_expression_exasol_safe(&legs.qualify(expr)?))
}

/// `Ok(None)` when absent, trivially true, or unrenderable; the caller must then self-apply
/// the filter (e.g. as an outer WHERE), never omit it. Exasol dialect, as above.
pub(super) fn render_df_filter_qualified(
    filter: &Json,
    legs: &JoinLegs,
) -> Result<Option<String>, UnattributableColumn> {
    Ok(render_df_filter_exasol_safe(&legs.qualify(filter)?))
}

/// An OR is never split, so an OR spanning legs stays withheld from every leg.
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

/// `None` when nothing is kept, the bare conjunct for one, else a `predicate_and`.
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

/// The conjuncts every column of which belongs to leg `leg` (by leg, never by table name,
/// so self-join occurrences get only their own). Makes no renderability decision. Sound
/// for an inner join: a single-leg conjunct is necessary for that leg's rows to survive.
///
/// Its consumers deliberately receive different trees:
/// (a) manifest pruning gets it raw, so every leg-local conjunct prunes even when
/// DataFusion cannot render it;
/// (b) the leg's `ScanSpec.filter` gets it screened by [`renderable_only`] and then
/// [`type_screened_leg_filter`];
/// (c) the outer wrapper's WHERE gets the raw type-declined conjuncts (Exasol dialect).
/// Cross-leg and OR-spanning conjuncts go only to the outer WHERE.
pub(super) fn leg_local_filter(filter: &Json, legs: &JoinLegs, leg: usize) -> Option<Json> {
    partition_conjuncts(filter, |c| legs.conjunct_leg(c) == Some(leg))
}

/// `None` when every conjunct is leg-local. An unattributable conjunct is withheld from every
/// leg and surfaces as a hard decline when the wrapper renders it, never guessed onto a leg.
/// It complements [`leg_local_filter`] over the tree it is given; only composed with
/// [`renderable_only`]/[`declined_only`] does the whole filter get applied exactly once.
pub(super) fn cross_leg_residual_filter(filter: &Json, legs: &JoinLegs) -> Option<Json> {
    partition_conjuncts(filter, |c| legs.conjunct_leg(c).is_none())
}

/// The sole renderability screen on the N-scan path, applied only at
/// [`super::sql_builders::build_n_scan_join_sql`]'s render sites, not inside
/// [`leg_local_filter`]: manifest pruning must see unscreened conjuncts, and dropping one
/// there would silently open more files with no test catching it.
pub(super) fn renderable_only(filter: &Json) -> Option<Json> {
    partition_conjuncts(filter, datafusion_renderable)
}

/// The complement of [`renderable_only`], applied by the outer wrapper's WHERE.
pub(super) fn declined_only(filter: &Json) -> Option<Json> {
    partition_conjuncts(filter, |c| !datafusion_renderable(c))
}

/// Returns `(leg_filter, type_declined)`: a total per-conjunct partition, the first half
/// rewritten for the leg, the second raw for the Exasol-dialect outer WHERE.
///
/// Not [`classify_where_filter`](super::super::support::classify_where_filter), which
/// classifies a whole filter against one type universe: the N-scan path has no
/// disjoint-name guarantee, so the only valid universe is the owning leg's `col_types`,
/// known only after attribution. Per conjunct, so one declined conjunct doesn't forfeit its
/// siblings. Renderability is established on the rewritten tree via
/// [`type_accepted_rewrite`]; if the re-formed tree fails it, the whole side-local set
/// becomes residual (fail closed: applied in the wrapper is slower, applied nowhere is wrong).
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

/// Inputs must be disjoint conjunct sets; nothing is de-duplicated.
pub(super) fn conjoin_filters(left: Option<Json>, right: Option<Json>) -> Option<Json> {
    match (left, right) {
        (Some(l), Some(r)) => Some(serde_json::json!({
            "type": "predicate_and",
            "expressions": [l, r],
        })),
        (l, r) => l.or(r),
    }
}

/// Leg-keyed, not name-keyed: self-join occurrences share one `tableName`.
fn collect_leg_column_names(
    expr: &Json,
    legs: &JoinLegs,
    leg: usize,
    out: &mut std::collections::HashSet<String>,
) {
    walk_column_nodes(expr, &mut |map| {
        if legs.resolve_column(map) == ColumnLeg::Leg(leg)
            && let Some(name) = map.get("name").and_then(|n| n.as_str())
        {
            out.insert(name.to_ascii_uppercase());
        }
    });
}

/// Keyed on table name, not leg: its caller checks format-reader refusals, which belong to
/// the table. Over-charging every occurrence, and untagged references to every side, is the
/// fail-safe direction.
pub(super) fn possible_side_column_names(
    expr: &Json,
    table_name: &str,
) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::new();
    walk_column_nodes(expr, &mut |map| {
        let Some(name) = map.get("name").and_then(|n| n.as_str()) else {
            return;
        };
        match map.get("tableName").and_then(|t| t.as_str()) {
            Some(tn) if !tn.eq_ignore_ascii_case(table_name) => {}
            _ => {
                names.insert(name.to_ascii_uppercase());
            }
        }
    });
    names
}

/// The projection then holds columns no `column` node names (`SELECT *` or the universe's
/// first column), so per-side consumers must charge them themselves.
pub(super) fn has_no_explicit_select_list(pushdown_req: &Json) -> bool {
    !matches!(pushdown_req.get("selectList"), Some(Json::Array(list)) if !list.is_empty())
}

/// The single owner of which clauses can name a source column. The collector is a
/// parameter because the callers must stay divergent: they fold case differently (see
/// `walk_column_nodes` and `vs-adapter/pushdown-module-structure`) and fall back differently.
/// [`referenced_leg_columns`]'s empty-`selectList` short-circuit must not move in here:
/// `referenced_column_projection` must keep narrowing through the remaining clauses
/// (`vs-adapter/pushdown-joins-module-structure`).
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

/// Drops columns the N-scan wrapper never references so each leg ships narrow rows. Kept:
/// this leg's columns in SELECT, the join condition, the full WHERE (the wrapper renders
/// all of it), GROUP BY, HAVING, and ORDER BY. An empty select list (`SELECT *`) or an empty
/// narrowing keeps `full_cols`.
///
/// CROSS-FOLD match: `full_cols` is Unicode-uppercased by `support::column_types`, `names`
/// ASCII-uppercased. They agree only because `build_listing_virtual_tables` Unicode-uppercases
/// every declared name (guarded by E2E `non_ascii_table_and_column_stay_queryable`); repair
/// any divergence at that premise, never by unifying the folds. A partial mismatch would
/// drop a referenced column, surfacing as an Exasol column-not-found error.
pub(super) fn referenced_leg_columns(
    pushdown_req: &Json,
    condition: &Json,
    legs: &JoinLegs,
    leg: usize,
    full_cols: &[(String, String)],
) -> Vec<(String, String)> {
    if has_no_explicit_select_list(pushdown_req) {
        return full_cols.to_vec();
    }
    let mut names = std::collections::HashSet::new();
    collect_leg_column_names(condition, legs, leg, &mut names);
    referenced_clause_values(pushdown_req, |v| {
        collect_leg_column_names(v, legs, leg, &mut names)
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
