use crate::scan::spec::{ColumnRef, ProjectionItem, relation_alias};
use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::error::UdfError;
use vs_expression::{Column, ColumnTypeLookup, Expr, ScanTyping, render_expression_exasol};

use super::super::context::PushdownContext;
use super::super::request::{Clause, OutputContract, PushdownRequest};
use super::super::support::{ColumnTypes, Emitted, ScanTranslator, quote_ident};
use super::attribution::JoinLegs;
use super::condition_filters::join_key_legs;
use super::plan::{JoinPlan, LegGroup};
use super::planning::ResolvedJoinSide;

/// Types a raw request column by the leg it belongs to, so a name two legs share takes its own
/// leg's type.
struct LegColumnTypes<'a> {
    legs: &'a JoinLegs,
    cols_per_leg: &'a [&'a ColumnTypes],
}

impl ColumnTypeLookup for LegColumnTypes<'_> {
    fn column_type(&self, column: &Column) -> Option<ExasolType> {
        let leg = self.legs.leg(column);
        self.cols_per_leg.get(leg)?.column_type(column)
    }
}

/// A qualified-wrapper select item's type, read from the raw request node, whose columns still
/// name their own tables.
pub(super) fn select_item_type(
    item: &Expr,
    legs: &JoinLegs,
    cols_per_leg: &[&ColumnTypes],
    context: &PushdownContext<'_>,
) -> Option<ExasolType> {
    ScanTyping::new(&LegColumnTypes { legs, cols_per_leg }, context.version).exact_type(item)
}

pub(super) fn projection_item_select_sql(item: &ProjectionItem) -> String {
    match item {
        ProjectionItem::Column(column) => quote_ident(&column.output_name()),
        ProjectionItem::Expr { expr } => expr.clone(),
    }
}

/// How an Exasol wrapper names each leg's column. A leg with a fan-out of its own is a relation of
/// the wrapper's FROM, `"LHS_T{leg}"."COLUMN"`. A leg of a broadcast fan-out is the fan-out's
/// output `"LHS_T{leg}_COLUMN"`, qualified with the fan-out's alias `LHS_T{sharded}` when Exasol
/// joins it with other fan-outs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LegNaming {
    per_leg: Vec<LegName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LegName {
    relation: Option<String>,
    prefix: Option<String>,
}

impl LegNaming {
    /// Every leg a relation of its own.
    pub(super) fn relations(leg_count: usize) -> Self {
        Self {
            per_leg: (0..leg_count)
                .map(|leg| LegName {
                    relation: Some(relation_alias(leg)),
                    prefix: None,
                })
                .collect(),
        }
    }

    /// The naming of `plan`'s fan-outs. A plan of one fan-out is the wrapper's unaliased FROM.
    pub(super) fn of_plan(plan: &JoinPlan, leg_count: usize) -> Self {
        let single = plan.fan_outs().len() == 1;
        let mut per_leg = Self::relations(leg_count).per_leg;
        for fan_out in plan.fan_outs().iter().filter(|fan_out| !fan_out.is_plain()) {
            for leg in fan_out.legs() {
                per_leg[leg] = LegName {
                    relation: (!single).then(|| relation_alias(fan_out.sharded)),
                    prefix: Some(relation_alias(leg)),
                };
            }
        }
        Self { per_leg }
    }

    /// `expr` in the Exasol dialect, each column named as its leg's.
    pub(super) fn render(&self, expr: &Expr, legs: &JoinLegs) -> Result<String, UdfError> {
        let named = expr.map_columns(&mut |column| {
            let leg = &self.per_leg[legs.leg(column)];
            let name = leg.column_name(&column.upper_name());
            Column::aliased(name, leg.relation.as_deref())
        });
        render_expression_exasol(&named)
    }

    pub(super) fn column(&self, leg: usize, name: &str) -> String {
        let leg = &self.per_leg[leg];
        let name = quote_ident(&leg.column_name(name));
        match &leg.relation {
            Some(relation) => format!("{}.{name}", quote_ident(relation)),
            None => name,
        }
    }
}

impl LegName {
    fn column_name(&self, name: &str) -> String {
        match &self.prefix {
            Some(prefix) => ColumnRef::qualified(prefix.clone(), name).output_name(),
            None => name.to_string(),
        }
    }
}

fn partition_conjuncts(filter: &Expr, keep: impl Fn(&Expr) -> bool) -> Option<Expr> {
    Expr::conjoin(
        filter
            .conjuncts()
            .into_iter()
            .filter(|&c| keep(c))
            .cloned()
            .collect(),
    )
}

/// The conjuncts that read only leg `leg`, raw, for that leg's file pruning: pruning by them is
/// sound for an inner join, even when the scan declines one.
pub(super) fn fan_out_filter(filter: &Expr, legs: &JoinLegs, leg: usize) -> Option<Expr> {
    partition_conjuncts(filter, |c| fan_out_holds(legs, &LegGroup::plain(leg), c))
}

/// Whether the scan of `fan_out` can apply the WHERE conjunct `conjunct`: every leg it reads lies
/// in the fan-out. A conjunct that reads no column holds for every row or none, so a broadcast
/// scan applies it, as the two-leg broadcast always has, and the N-scan leaves it to Exasol.
pub(super) fn fan_out_holds(legs: &JoinLegs, fan_out: &LegGroup, conjunct: &Expr) -> bool {
    let read = legs.legs_referenced(conjunct);
    match read.is_empty() {
        true => !fan_out.is_plain(),
        false => read.iter().all(|&leg| fan_out.holds(leg)),
    }
}

/// Where each join condition and WHERE conjunct of a join runs under a plan, before the scan
/// screen. Every WHERE conjunct lies in exactly one fan-out's local filter or in the residual.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ConditionPlacement {
    /// The broadcast pair's join keys, which its join block runs. `None` without a pair.
    pub pair_keys: Option<Expr>,
    /// Per chain position, the join conditions its `ON` runs.
    pub on: Vec<Vec<Expr>>,
    /// Per fan-out, the WHERE conjuncts whose legs all lie in it.
    pub local: Vec<Option<Expr>>,
    /// The WHERE conjuncts no fan-out holds, which Exasol's WHERE runs.
    pub residual: Option<Expr>,
}

/// A condition that holds a pair key and something else splits, and the rest keeps its own join
/// point. Every other condition stays whole, so the N-scan renders each as written.
pub(super) fn place_conditions(
    filter: Option<&Expr>,
    conditions: &[Expr],
    legs: &JoinLegs,
    plan: &JoinPlan,
) -> ConditionPlacement {
    let pair = plan.fan_outs().iter().find(|fan_out| !fan_out.is_plain());
    let is_pair_key = |conjunct: &Expr| {
        pair.is_some_and(|pair| {
            join_key_legs(conjunct, legs).is_some_and(|(a, b)| pair.legs() == [a, b])
        })
    };
    let mut pair_keys = Vec::new();
    let mut chained = Vec::new();
    for condition in conditions {
        let (keys, rest): (Vec<&Expr>, Vec<&Expr>) = condition
            .conjuncts()
            .into_iter()
            .partition(|conjunct| is_pair_key(conjunct));
        match (keys.is_empty(), rest.is_empty()) {
            (true, _) => chained.push(condition.clone()),
            (false, true) => pair_keys.push(condition.clone()),
            (false, false) => {
                pair_keys.extend(keys.into_iter().cloned());
                chained.extend(Expr::conjoin(rest.into_iter().cloned().collect()));
            }
        }
    }

    let mut on = vec![Vec::new(); plan.fan_outs().len()];
    let mut unattached = Vec::new();
    for condition in chained {
        let position = legs
            .legs_referenced(&condition)
            .iter()
            .map(|&leg| plan.position(leg))
            .max()
            .unwrap_or_default();
        // Only a pair can hold every leg of a condition at the first position: the WHERE runs it.
        match position {
            0 => unattached.push(condition),
            _ => on[position].push(condition),
        }
    }

    let local = plan
        .fan_outs()
        .iter()
        .map(|fan_out| {
            filter.and_then(|f| partition_conjuncts(f, |c| fan_out_holds(legs, fan_out, c)))
        })
        .collect();
    let structural = filter.and_then(|f| {
        partition_conjuncts(f, |c| {
            !plan
                .fan_outs()
                .iter()
                .any(|fan_out| fan_out_holds(legs, fan_out, c))
        })
    });
    ConditionPlacement {
        pair_keys: Expr::conjoin(pair_keys),
        on,
        local,
        residual: conjoin_filters(structural, Expr::conjoin(unattached)),
    }
}

/// Returns `(leg_filter_sql, declined)` through [`ScanTranslator::screen_conjuncts`] against the
/// leg's own columns, so one declined conjunct costs only itself. `Err` as
/// [`ScanTranslator::over`].
pub(super) fn screen_leg_filter(
    side_local: &Expr,
    col_types: &ColumnTypes,
    context: &PushdownContext<'_>,
) -> Result<(Option<String>, Option<Expr>), UdfError> {
    // Bare: the fan-out relation exposes bare uppercase names, so an alias would not resolve.
    let scan = ScanTranslator::over(
        col_types,
        context.version,
        &[&side_local.strip_table_alias()],
    )?;
    Ok(scan.screen_conjuncts(side_local, Expr::strip_table_alias))
}

/// The pair's local conjuncts, split into the scan filter and the raw conjuncts the scan
/// declines. A filter that renders whole keeps its one rendered SQL.
pub(super) fn screen_pair_filter(
    local: &Expr,
    legs: &JoinLegs,
    sides: &[ResolvedJoinSide],
    context: &PushdownContext<'_>,
) -> Result<(Option<String>, Option<Expr>), UdfError> {
    let col_types = leg_column_types(legs, sides);
    let qualified = legs.qualify(local);
    let scan = ScanTranslator::over(&col_types, context.version, &[&qualified])?;
    if let Some(sql) = scan.filter(&qualified) {
        return Ok((Some(sql), None));
    }
    Ok(scan.screen_conjuncts(local, |conjunct| legs.qualify(conjunct)))
}

/// Every leg's columns, keyed by its relation alias.
pub(super) fn leg_column_types(legs: &JoinLegs, sides: &[ResolvedJoinSide]) -> ColumnTypes {
    ColumnTypes::per_relation(
        sides
            .iter()
            .enumerate()
            .map(|(leg, side)| (legs.leg_alias(leg), &side.column_types)),
    )
}

/// Inputs must be disjoint conjunct sets; nothing is de-duplicated.
pub(super) fn conjoin_filters(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (Some(l), Some(r)) => Some(Expr::And(vec![l, r])),
        (l, r) => l.or(r),
    }
}

/// Leg-keyed, not name-keyed: self-join occurrences share one `tableName`.
fn collect_leg_column_names(
    expr: &Expr,
    legs: &JoinLegs,
    leg: usize,
    out: &mut std::collections::HashSet<String>,
) {
    out.extend(
        expr.columns()
            .into_iter()
            .filter(|column| legs.leg(column) == leg)
            .map(Column::ascii_upper_name),
    );
}

/// Keyed on table name, not leg: its caller checks format-reader refusals, which belong to
/// the table. Over-charging every occurrence is the fail-safe direction.
pub(super) fn possible_side_column_names(
    pushdown: &PushdownRequest,
    table_name: &str,
) -> std::collections::HashSet<String> {
    pushdown
        .expressions(&Clause::ALL)
        .into_iter()
        .flat_map(Expr::columns)
        .filter(|column| {
            column
                .table_name
                .as_deref()
                .is_some_and(|tn| tn.eq_ignore_ascii_case(table_name))
        })
        .map(Column::ascii_upper_name)
        .collect()
}

/// Leg `leg`'s columns that `values` name, in declaration order. CROSS-FOLD: `full_cols` is
/// Unicode-uppercased and `names` ASCII-uppercased; fix a divergence where names are declared.
pub(super) fn leg_columns_named_in<'a>(
    values: impl IntoIterator<Item = &'a Expr>,
    legs: &JoinLegs,
    leg: usize,
    full_cols: &ColumnTypes,
) -> ColumnTypes {
    let mut names = std::collections::HashSet::new();
    for value in values {
        collect_leg_column_names(value, legs, leg, &mut names);
    }
    full_cols.narrowed(|column| names.contains(&column.name))
}

/// The one owner of the columns a fan-out over `scanned` emits per leg: every column for
/// `SELECT *`, else the columns the clauses `above` it name. Exasol only counts the rows of a
/// fan-out whose clauses name none, so its first leg then emits one placeholder, never its row,
/// which may hold a refused column.
pub(super) fn fan_out_emission(
    output: OutputContract,
    above: &[&Expr],
    legs: &JoinLegs,
    scanned: &[(usize, &ColumnTypes)],
    prefer: &ColumnTypes,
) -> Result<Vec<Emitted>, UdfError> {
    let mut emitted = scanned
        .iter()
        .map(|&(leg, cols)| match output {
            OutputContract::WholeRow => cols.named(|_| true),
            OutputContract::AnyOneColumn | OutputContract::Items => {
                let named = leg_columns_named_in(above.iter().copied(), legs, leg, cols);
                cols.named(|column| named.get(column).is_some())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if emitted.iter().all(Emitted::is_empty)
        && let Some((_, cols)) = scanned.first()
    {
        emitted[0] = cols.placeholder(prefer)?;
    }
    Ok(emitted)
}

/// [`fan_out_emission`] of a fan-out that scans the one leg `leg`.
pub(super) fn leg_emission(
    output: OutputContract,
    above: &[&Expr],
    legs: &JoinLegs,
    leg: usize,
    cols: &ColumnTypes,
) -> Result<Emitted, UdfError> {
    let mut emitted =
        fan_out_emission(output, above, legs, &[(leg, cols)], &ColumnTypes::default())?;
    Ok(emitted.remove(0))
}

#[cfg(test)]
#[path = "rendering_tests.rs"]
mod tests;
