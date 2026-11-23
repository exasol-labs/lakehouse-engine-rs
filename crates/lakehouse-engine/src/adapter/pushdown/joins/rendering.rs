use crate::scan::spec::ProjectionItem;
use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{
    ColumnNode, ColumnTypeLookup, ScanTyping, render_expression_exasol, strip_table_alias,
    walk_column_nodes,
};

use super::super::context::PushdownContext;
use super::super::request::OutputContract;
use super::super::support::{
    ColumnTypes, Emitted, ScanTranslator, conjoin, flatten_conjuncts, quote_ident,
};
use super::attribution::JoinLegs;

/// Types a raw request column by the leg it belongs to, so a name two legs share takes its own
/// leg's type.
struct LegColumnTypes<'a> {
    legs: &'a JoinLegs,
    cols_per_leg: &'a [&'a ColumnTypes],
}

impl ColumnTypeLookup for LegColumnTypes<'_> {
    fn column_type(&self, column: &Json) -> Option<ExasolType> {
        let leg = self.legs.leg(ColumnNode::parse(column)?);
        self.cols_per_leg.get(leg)?.column_type(column)
    }
}

/// A qualified-wrapper select item's type, read from the raw request node, whose columns still
/// name their own tables.
pub(super) fn select_item_type(
    item: &Json,
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

/// Stamps each `column` with its leg's subquery alias, then renders in the Exasol dialect,
/// since the N-scan wrapper is parsed by Exasol (length-qualified `VARCHAR(n)` CAST targets,
/// unlike the DataFusion-dialect broadcast render).
pub(super) fn render_expression_qualified(expr: &Json, legs: &JoinLegs) -> String {
    render_expression_exasol(&legs.qualify(expr))
}

/// How an Exasol wrapper names a leg's column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LegColumns {
    /// `"LHS_T{leg}"."COLUMN"`: each leg is a relation of the wrapper's FROM.
    Relations,
    /// `"LHS_T{leg}_COLUMN"`: one broadcast fan-out emits every leg's columns.
    FanOutOutput,
}

impl LegColumns {
    pub(super) fn render(self, expr: &Json, legs: &JoinLegs) -> String {
        match self {
            Self::Relations => render_expression_qualified(expr, legs),
            Self::FanOutOutput => render_expression_exasol(&legs.to_fan_out_output(expr)),
        }
    }

    pub(super) fn column(self, legs: &JoinLegs, leg: usize, name: &str) -> String {
        match self {
            Self::Relations => format!(
                "{}.{}",
                quote_ident(&legs.leg_alias(leg)),
                quote_ident(name)
            ),
            Self::FanOutOutput => quote_ident(&legs.fan_out_name(leg, name)),
        }
    }
}

fn partition_conjuncts(filter: &Json, keep: impl Fn(&Json) -> bool) -> Option<Json> {
    conjoin(
        flatten_conjuncts(filter)
            .into_iter()
            .filter(|&c| keep(c))
            .cloned()
            .collect(),
    )
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

/// Returns `(leg_filter_sql, declined)` through [`ScanTranslator::screen_conjuncts`] against the
/// leg's own columns, so one declined conjunct costs only itself. `Err` as
/// [`ScanTranslator::over`].
pub(super) fn screen_leg_filter(
    side_local: &Json,
    col_types: &ColumnTypes,
    context: &PushdownContext<'_>,
) -> Result<(Option<String>, Option<Json>), UdfError> {
    // Bare: the fan-out relation exposes bare uppercase names, so an alias would not resolve.
    let scan = ScanTranslator::over(
        col_types,
        context.version,
        &[&strip_table_alias(side_local)],
    )?;
    Ok(scan.screen_conjuncts(side_local, strip_table_alias))
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
        if legs.leg(column) == leg {
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

/// Leg `leg`'s columns that `values` name, in declaration order. CROSS-FOLD: `full_cols` is
/// Unicode-uppercased and `names` ASCII-uppercased; fix a divergence where names are declared.
pub(super) fn leg_columns_named_in<'a>(
    values: impl IntoIterator<Item = &'a Json>,
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

/// What the clauses `above` a fan-out make it emit per leg of `scanned`: every column for
/// `SELECT *`, else the columns they name.
pub(super) fn named_emission(
    output: OutputContract,
    above: &[&Json],
    legs: &JoinLegs,
    scanned: &[(usize, &ColumnTypes)],
) -> Result<Vec<Emitted>, UdfError> {
    scanned
        .iter()
        .map(|&(leg, cols)| match output {
            OutputContract::WholeRow => cols.named(|_| true),
            OutputContract::AnyOneColumn | OutputContract::Items => {
                let named = leg_columns_named_in(above.iter().copied(), legs, leg, cols);
                cols.named(|column| named.get(column).is_some())
            }
        })
        .collect()
}

/// A fan-out must emit a column, and Exasol only counts the rows of one whose clauses name
/// none. Its first leg then emits one placeholder, never its row, which may hold a refused column.
pub(super) fn with_placeholder(
    mut emitted: Vec<Emitted>,
    scanned: &[(usize, &ColumnTypes)],
    prefer: &ColumnTypes,
) -> Result<Vec<Emitted>, UdfError> {
    if emitted.iter().all(Emitted::is_empty)
        && let Some((_, cols)) = scanned.first()
    {
        emitted[0] = cols.placeholder(prefer)?;
    }
    Ok(emitted)
}

/// The one owner of the columns a fan-out over `scanned` emits per leg.
pub(super) fn fan_out_emission(
    output: OutputContract,
    above: &[&Json],
    legs: &JoinLegs,
    scanned: &[(usize, &ColumnTypes)],
    prefer: &ColumnTypes,
) -> Result<Vec<Emitted>, UdfError> {
    let named = named_emission(output, above, legs, scanned)?;
    with_placeholder(named, scanned, prefer)
}

/// [`fan_out_emission`] of a fan-out that scans the one leg `leg`.
pub(super) fn leg_emission(
    output: OutputContract,
    above: &[&Json],
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
