use crate::scan::spec::ProjectionItem;
use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{
    ColumnNode, ColumnTypeLookup, conjoin_scan_filters, exact_expression_type,
    render_df_filter_exasol_safe, render_expression_exasol_safe, strip_table_alias,
    walk_column_nodes,
};

use super::super::support::{ColumnTypes, ScanTranslator, quote_ident};
use super::attribution::{ColumnLeg, JoinLegs};

/// Types a raw request column by the leg it belongs to, so a name two legs share takes its own
/// leg's type.
struct LegColumnTypes<'a> {
    legs: &'a JoinLegs,
    cols_per_leg: &'a [&'a ColumnTypes],
}

impl ColumnTypeLookup for LegColumnTypes<'_> {
    fn column_type(&self, column: &Json) -> Option<ExasolType> {
        let leg = self
            .legs
            .bind(ColumnNode::parse(column)?)
            .ok()?
            .unwrap_or(0);
        self.cols_per_leg.get(leg)?.column_type(column)
    }
}

/// A qualified-wrapper select item's type, read from the raw request node, whose columns still
/// name their own tables.
pub(super) fn select_item_type(
    item: &Json,
    legs: &JoinLegs,
    cols_per_leg: &[&ColumnTypes],
) -> Option<ExasolType> {
    exact_expression_type(item, &LegColumnTypes { legs, cols_per_leg })
}

pub(super) fn projection_item_select_sql(item: &ProjectionItem) -> String {
    match item {
        ProjectionItem::Column(column) => quote_ident(&column.output_name()),
        ProjectionItem::Expr { expr } => expr.clone(),
    }
}

/// Stamps each `column` with its leg's subquery alias, then renders in the Exasol dialect,
/// since the N-scan wrapper is parsed by Exasol (length-qualified `VARCHAR(n)` CAST targets,
/// unlike the DataFusion-dialect broadcast render). `Err` as [`JoinLegs::qualify`].
pub(super) fn render_expression_qualified(
    expr: &Json,
    legs: &JoinLegs,
) -> Result<Option<String>, UdfError> {
    Ok(render_expression_exasol_safe(&legs.qualify(expr)?))
}

/// `Ok(None)` when absent, trivially true, or unrenderable; the caller must then self-apply
/// the filter (e.g. as an outer WHERE), never omit it. Exasol dialect, as above.
pub(super) fn render_df_filter_qualified(
    filter: &Json,
    legs: &JoinLegs,
) -> Result<Option<String>, UdfError> {
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

fn partition_conjuncts(filter: &Json, keep: impl Fn(&Json) -> bool) -> Option<Json> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(filter, &mut conjuncts);
    conjoin(
        conjuncts
            .into_iter()
            .filter(|&c| keep(c))
            .cloned()
            .collect(),
    )
}

/// `None` when nothing is kept, the bare conjunct for one, else a `predicate_and`.
fn conjoin(mut kept: Vec<Json>) -> Option<Json> {
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
/// (b) the leg's `ScanSpec.filter` gets the conjuncts [`screen_leg_filter`] renders;
/// (c) the outer wrapper's WHERE gets the raw declined conjuncts (Exasol dialect).
/// Cross-leg and OR-spanning conjuncts go only to the outer WHERE.
pub(super) fn leg_local_filter(filter: &Json, legs: &JoinLegs, leg: usize) -> Option<Json> {
    partition_conjuncts(filter, |c| legs.conjunct_leg(c) == Some(leg))
}

/// `None` when every conjunct is leg-local. It complements [`leg_local_filter`] over the tree it is given.
pub(super) fn cross_leg_residual_filter(filter: &Json, legs: &JoinLegs) -> Option<Json> {
    partition_conjuncts(filter, |c| legs.conjunct_leg(c).is_none())
}

/// Returns `(leg_filter_sql, declined)`. Each conjunct renders once against the leg's own
/// columns, so one declined conjunct costs only itself and no later render can decline the rest.
/// `Err` as [`ScanTranslator::over`].
pub(super) fn screen_leg_filter(
    side_local: &Json,
    col_types: &ColumnTypes,
) -> Result<(Option<String>, Option<Json>), UdfError> {
    // Bare: the fan-out relation exposes bare uppercase names, so an alias would not resolve.
    let bare = strip_table_alias(side_local);
    let scan = ScanTranslator::over(col_types, &[&bare])?;
    let (mut bare_conjuncts, mut conjuncts) = (Vec::new(), Vec::new());
    flatten_conjuncts(&bare, &mut bare_conjuncts);
    flatten_conjuncts(side_local, &mut conjuncts);
    let mut rendered = Vec::new();
    let mut declined = Vec::new();
    for (bare, conjunct) in bare_conjuncts.into_iter().zip(conjuncts) {
        match scan.expression(bare) {
            Some(fragment) => rendered.push(fragment.sql),
            None => declined.push(conjunct.clone()),
        }
    }
    Ok((conjoin_scan_filters(rendered), conjoin(declined)))
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
    walk_column_nodes(expr, &mut |column| {
        if legs.resolve_column(column) == ColumnLeg::Leg(leg) {
            out.insert(column.ascii_upper_name());
        }
    });
}

/// Keyed on table name, not leg: its caller checks format-reader refusals, which belong to
/// the table. Over-charging every occurrence is the fail-safe direction.
pub(super) fn possible_side_column_names(
    expr: &Json,
    table_name: &str,
) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::new();
    walk_column_nodes(expr, &mut |column| {
        if column
            .table_name
            .is_some_and(|tn| tn.eq_ignore_ascii_case(table_name))
        {
            names.insert(column.ascii_upper_name());
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
/// `ColumnNode::ascii_upper_name`) and fall back differently.
/// [`referenced_leg_columns`]'s empty-`selectList` short-circuit must not move in here:
/// `referenced_columns` must keep narrowing through the remaining clauses.
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
/// CROSS-FOLD match: `full_cols` is Unicode-uppercased by `support::involved_columns`, `names`
/// ASCII-uppercased. They agree only because `build_listing_virtual_tables` Unicode-uppercases
/// every declared name (guarded by E2E `non_ascii_table_and_column_stay_queryable`); repair
/// any divergence at that premise, never by unifying the folds. A partial mismatch would
/// drop a referenced column, surfacing as an Exasol column-not-found error.
pub(super) fn referenced_leg_columns(
    pushdown_req: &Json,
    condition: &Json,
    legs: &JoinLegs,
    leg: usize,
    full_cols: &ColumnTypes,
) -> ColumnTypes {
    if has_no_explicit_select_list(pushdown_req) {
        return full_cols.clone();
    }
    let mut names = std::collections::HashSet::new();
    collect_leg_column_names(condition, legs, leg, &mut names);
    referenced_clause_values(pushdown_req, |v| {
        collect_leg_column_names(v, legs, leg, &mut names)
    });
    let narrowed = full_cols.narrowed(|column| names.contains(&column.name));
    if narrowed.is_empty() {
        full_cols.clone()
    } else {
        narrowed
    }
}

#[cfg(test)]
#[path = "rendering_tests.rs"]
mod tests;
