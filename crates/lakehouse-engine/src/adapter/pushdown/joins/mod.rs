use crate::adapter::permission::PermissionCheck;
use crate::adapter::statement_state::StatementCatalog;
#[cfg(test)]
use crate::scan::spec::StorageBackend;
use exasol_udf_sdk::error::UdfError;
use futures::future::try_join_all;
use serde_json::Value as Json;
use vs_expression::Expr;

use super::context::PushdownContext;
use super::empty_result::empty_result_sql;
use super::refused_columns::ensure_no_touched_column_is_refused;
use super::request::{AdapterRequest, Clause, OutputContract, PushdownRequest};
use super::scan_resolution::TableScanResolver;
use super::support::ScanTranslator;
use super::support::{ColumnTypes, InvolvedColumns, project_columns, pushdown_response};

mod attribution;
mod broadcast_policy;
mod condition_filters;
mod plan;
mod planning;
mod rendering;
mod sql_builders;
mod where_placement;

pub(crate) use planning::{DetectedJoin, JoinLeaf, JoinShape, ResolvedJoinSide, detect_join};
pub(crate) use sql_builders::RenderedJoinPushdown;

pub(super) use sql_builders::{
    EmptyRelation, n_scan_emission, qualified_single_table_fallback_pushdown, single_table_emission,
};
// The dispatch-golden sibling module drives the plan renderer directly.
pub(super) use plan::{JoinPlan, LegGroup};
pub(super) use sql_builders::render_join_plan;
#[cfg(test)]
pub(super) use sql_builders::{build_pair_fan_out_sql, build_qualified_single_table_fallback_sql};
pub(super) use where_placement::WherePlacement;

use broadcast_policy::{JoinGraph, WhereConjunct, group_legs};
use plan::broadcast_serves_window;
#[cfg(test)]
pub(super) use planning::JoinWindowPlan;
pub(super) use planning::involved_table;
use planning::resolve_one_join_side;
use rendering::{fan_out_filter, leg_column_types, possible_side_column_names, screen_pair_filter};
use sql_builders::ShardAggregate;
use sql_builders::qualified_emission;

/// Fails the request unless every `column` node of `pushdown` binds one table of the query, a
/// leg of `join` or the request's one table, and that table declares it (#615). `declared` holds
/// each leg's columns in leg order, as the caller already read them from `involvedTables`. Both
/// planners run it before any catalog request.
pub(crate) fn ensure_columns_declared(
    request: &AdapterRequest,
    pushdown: &PushdownRequest,
    join: Option<&DetectedJoin>,
    declared: &[&InvolvedColumns<'_>],
) -> Result<(), UdfError> {
    let legs = match join {
        Some(join) => join.legs(),
        None => attribution::JoinLegs::for_single_scan(request),
    };
    legs.ensure_columns_declared(pushdown, declared)
}

/// Attribution is per side: a name refused on one side must not refuse a query reading only
/// the other side's same-named column. `SELECT *` names no column, so it charges every one.
fn ensure_no_side_refuses_a_referenced_column(
    request: &AdapterRequest,
    output: &OutputContract,
    sides: &[ResolvedJoinSide],
) -> Result<(), UdfError> {
    for side in sides {
        let mut touched = possible_side_column_names(request.pushdown(), &side.table_name);
        match output {
            OutputContract::WholeRow => touched.extend(
                side.scan
                    .refused_columns
                    .iter()
                    .map(|column| column.column_name.to_uppercase()),
            ),
            OutputContract::AnyOneColumn | OutputContract::Items(_) => {}
        }
        ensure_no_touched_column_is_refused(&touched, &side.scan.refused_columns)?;
    }
    Ok(())
}

/// The empty result of a join with a fileless side. Every column binds leg-qualified, so a name
/// two legs share reads its own leg's type and refusal. It evaluates the whole WHERE, so every
/// column the WHERE names is emitted.
fn empty_join_sql(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    context: &PushdownContext<'_>,
) -> Result<Json, UdfError> {
    let emitted = n_scan_emission(pushdown, &WherePlacement::whole(pushdown), join, sides)?;
    let legs = join.legs();
    let col_types = leg_column_types(&legs, sides);
    let bound = pushdown.map_expressions(&Clause::ALL, |tree| legs.qualify(tree));
    let scan = ScanTranslator::over(
        &col_types,
        context.version,
        &bound.expressions(&Clause::ALL),
    )?;
    let projection = project_columns(&bound, scan, &qualified_emission(&legs, &emitted))?;
    empty_result_sql(
        pushdown,
        &bound,
        &projection,
        scan,
        &EmptyRelation::join(join, emitted),
        context,
    )
}

/// Plans the request with each inner join's `ON` filters moved to WHERE
/// ([`condition_filters::move_condition_filters`]), so every step below reads them there.
/// Resolves each table once (never per shard), pruned by its leg-local WHERE conjuncts. Any
/// empty side short-circuits to the shape-correct empty result, since an inner join is then
/// empty.
///
/// Broadcast is an optimization inside this one path: the first eligible pair whose fan-out
/// renders is broadcast, and every other leg keeps its own fan-out. The legs may share column
/// names or be one table joined to itself. With no such pair every leg keeps its own fan-out,
/// the N-scan, and Exasol joins them.
/// A column that binds no single leg, or that its leg does not declare, fails the request
/// before any catalog call ([`ensure_columns_declared`]).
///
/// No plan-time check compares the sides' storage backends: each side reads through its own
/// store. The one unservable collapse (two sides on one DataFusion registry key needing
/// different stores) is owned by the scan's `validate_sides_share_one_store`.
pub(super) async fn plan_join(
    request: &AdapterRequest,
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    statement: &StatementCatalog,
    permission: &PermissionCheck,
    context: &PushdownContext<'_>,
) -> Result<Json, UdfError> {
    // Legs are indexed positionally, not by tableName, which a self-join's occurrences share.
    let legs = join.legs();
    let involved = join
        .tables
        .iter()
        .map(|leaf| involved_table(request, &leaf.table_name))
        .collect::<Vec<_>>();
    // Before the move, whose leg lookups rely on every column binding a leg.
    ensure_columns_declared(
        request,
        pushdown,
        Some(join),
        &involved.iter().collect::<Vec<_>>(),
    )?;
    let moved = match condition_filters::move_condition_filters(pushdown, &legs) {
        Some(moved) => {
            let JoinShape::Join(moved_join) = detect_join(request, &moved)? else {
                unreachable!("moving condition filters keeps every join node");
            };
            Some((moved, moved_join))
        }
        None => None,
    };
    let (pushdown, join) = match &moved {
        Some((moved, moved_join)) => (moved, moved_join),
        None => (pushdown, join),
    };
    let filter = pushdown.filter();
    // Built once for every leg, so every leg's identifier passes its rule before any catalog
    // request.
    let identifiers: Vec<&str> = join
        .tables
        .iter()
        .map(|leaf| leaf.table_identifier.as_str())
        .collect();
    let resolver =
        TableScanResolver::for_request(statement, permission, &identifiers, context).await?;
    let side_filters: Vec<Option<Expr>> = (0..join.tables.len())
        .map(|leg| filter.and_then(|f| fan_out_filter(f, &legs, leg)))
        .collect();
    // `try_join_all` preserves leg order, which later steps index by.
    let sides: Vec<ResolvedJoinSide> =
        try_join_all(join.tables.iter().zip(&side_filters).zip(&involved).map(
            |((leaf, side_filter), involved)| {
                resolve_one_join_side(
                    &leaf.table_name,
                    &leaf.table_identifier,
                    &resolver,
                    side_filter.as_ref(),
                    involved,
                )
            },
        ))
        .await?;

    // Both before the zero-files return: a refused column must error, never yield an empty
    // result.
    ensure_no_side_refuses_a_referenced_column(request, pushdown.output(), &sides)?;

    if sides.iter().any(|s| s.scan.files.is_empty()) {
        return empty_join_sql(pushdown, join, &sides, context);
    }

    let sql = render_resolved_join(pushdown, join, &sides, context)?;
    Ok(pushdown_response(sql))
}

/// The SQL of a join whose every leg has files: the plan that broadcasts the first eligible pair
/// whose fan-out renders, else the N-scan.
fn render_resolved_join(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    context: &PushdownContext<'_>,
) -> Result<String, UdfError> {
    let leg_count = sides.len();
    for pair in broadcast_candidates(pushdown, join, sides, context)? {
        let plan = JoinPlan::broadcasting(leg_count, &pair);
        if let Some(sql) = render_join_plan(pushdown, join, sides, &plan, context)? {
            return Ok(sql);
        }
    }
    let n_scan = JoinPlan::n_scan(leg_count);
    Ok(render_join_plan(pushdown, join, sides, &n_scan, context)?
        .expect("the N-scan renders every join"))
}

/// The pairs worth broadcasting, best first ([`group_legs`]). None when a two-leg broadcast
/// cannot serve the request's window (#675).
fn broadcast_candidates(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    context: &PushdownContext<'_>,
) -> Result<Vec<LegGroup>, UdfError> {
    let legs = join.legs();
    let trees: Vec<&Expr> = pushdown.filter().map(Expr::conjuncts).unwrap_or_default();
    let conjuncts = trees
        .into_iter()
        .map(|tree| {
            Ok(WhereConjunct {
                tree,
                renders: screen_pair_filter(tree, &legs, sides, context)?.1.is_none(),
            })
        })
        .collect::<Result<Vec<_>, UdfError>>()?;
    let declined = conjuncts.iter().any(|conjunct| !conjunct.renders);
    if !broadcast_serves_window(pushdown, &legs, sides.len(), declined) {
        return Ok(Vec::new());
    }
    let bytes: Vec<u64> = sides.iter().map(|side| side.total_bytes).collect();
    let columns: Vec<&ColumnTypes> = sides.iter().map(|side| &side.column_types).collect();
    let above: Vec<&Expr> = pushdown.expressions(&Clause::WRAPPER);
    let shards_aggregate = ShardAggregate::of(pushdown, &legs, sides, context)?.is_some();
    Ok(group_legs(&JoinGraph {
        legs: &legs,
        bytes: &bytes,
        columns: &columns,
        conditions: &join.conditions,
        conjuncts: &conjuncts,
        above: &above,
        output: pushdown.output(),
        aggregates: pushdown.aggregation_type().is_some(),
        shard_keys: shards_aggregate.then(|| pushdown.group_by()),
        max_bytes: context.tuning.join_broadcast_max_bytes,
    }))
}

#[cfg(test)]
#[path = "joins_tests.rs"]
mod tests;
