use crate::types::exasol_type::ExasolType;
use serde_json::Value as Json;
use vs_expression::ColumnNode;

use super::super::support::ColumnTypes;
use super::attribution::JoinLegs;
use super::rendering::flatten_conjuncts;

/// Why a join the broadcast path can serve keeps the N-scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NScanReason {
    /// The dimension's file bytes exceed `JOIN_BROADCAST_MAX_BYTES`.
    OverBudget,
    /// The aggregation reads a dimension column that is not width-bounded.
    UnboundedDimensionColumn,
}

/// What the policy judges. Nothing rendered: a render decline is a capability limit, which the
/// builders own.
pub(super) struct BroadcastFacts<'a> {
    pub dimension_bytes: u64,
    pub max_bytes: u64,
    pub legs: &'a JoinLegs,
    pub dimension_leg: usize,
    /// The whole WHERE.
    pub filter: Option<&'a Json>,
    /// The dimension columns the select list, `GROUP BY`, `HAVING`, and `ORDER BY` read. `None`
    /// when the request does not aggregate.
    pub aggregation_reads: Option<&'a ColumnTypes>,
}

/// The one choice between the broadcast and the N-scan for a join both can serve. Rules run in
/// order, and the first that objects names the reason.
pub(super) fn n_scan_reason(facts: &BroadcastFacts<'_>) -> Option<NScanReason> {
    if facts.dimension_bytes > facts.max_bytes {
        return Some(NScanReason::OverBudget);
    }
    if let Some(read) = facts.aggregation_reads
        && !width_bounded(facts, read)
    {
        return Some(NScanReason::UnboundedDimensionColumn);
    }
    None
}

/// The fan-out repeats `read` once per joined row. Only a fixed-width type or a literal-bounding
/// WHERE conjunct keeps such a column narrow.
fn width_bounded(facts: &BroadcastFacts<'_>, read: &ColumnTypes) -> bool {
    let bounded_text = literal_bounded_columns(facts.filter, facts.legs, facts.dimension_leg);
    read.iter().all(|(column, column_type)| {
        matches!(
            column_type.exa_type,
            ExasolType::Boolean
                | ExasolType::Decimal(_)
                | ExasolType::DoublePrecision
                | ExasolType::Date
                | ExasolType::Timestamp { .. }
        ) || (column_type.exa_type.is_character() && bounded_text.contains(&column.name))
    })
}

/// Leg `leg`'s columns that a top-level WHERE conjunct `col = 'x'` or `col IN ('x', 'y')`
/// limits to string literals.
fn literal_bounded_columns(
    filter: Option<&Json>,
    legs: &JoinLegs,
    leg: usize,
) -> std::collections::HashSet<String> {
    let is_literal =
        |node: &Json| node.get("type").and_then(|t| t.as_str()) == Some("literal_string");
    let leg_column = |node: &Json| {
        ColumnNode::parse(node)
            .filter(|column| legs.leg(*column) == leg)
            .map(|column| column.ascii_upper_name())
    };
    let mut conjuncts = Vec::new();
    if let Some(filter) = filter {
        flatten_conjuncts(filter, &mut conjuncts);
    }
    conjuncts
        .into_iter()
        .filter_map(|conjunct| match conjunct.get("type")?.as_str()? {
            "predicate_equal" => {
                let (left, right) = (conjunct.get("left")?, conjunct.get("right")?);
                if is_literal(right) {
                    leg_column(left)
                } else if is_literal(left) {
                    leg_column(right)
                } else {
                    None
                }
            }
            "predicate_in_constlist" => {
                let values = conjunct.get("arguments")?.as_array()?;
                if values.is_empty() || !values.iter().all(is_literal) {
                    return None;
                }
                leg_column(conjunct.get("expression")?)
            }
            _ => None,
        })
        .collect()
}

#[cfg(test)]
#[path = "broadcast_policy_tests.rs"]
mod tests;
