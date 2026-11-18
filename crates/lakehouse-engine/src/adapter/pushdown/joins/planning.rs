use crate::adapter::table_identity::read_identifier;
use crate::scan::spec::JoinType;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::super::ResolvedScan;
use super::super::scan_resolution::TableScanResolver;
use super::super::support::{
    ColumnTypes, InvolvedColumns, extract_limit, extract_offset, involved_columns,
};
use super::super::topn::{ParsedSortKey, parse_sort_key_element};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinLeaf {
    pub table_name: String,
    /// Verbatim (Exasol does not fold it). `None` when absent: Exasol omits the key, and an
    /// alias-less occurrence is a distinct leg identity.
    pub table_alias: Option<String>,
    /// Original-cased catalog identifier from the identifier note of the leg's involved table.
    pub table_identifier: String,
    /// On the nullable side of an outer join node: a WHERE conjunct over it is not necessary
    /// for its rows to survive, so it never prunes or filters this leg.
    pub null_supplying: bool,
}

/// `conditions` are AND-conjoined by the N-scan fallback, which is order-agnostic for an
/// all-inner join. A tree with an outer node is rendered from `tree` instead.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DetectedJoin {
    /// Stable left-to-right tree order.
    pub tables: Vec<JoinLeaf>,
    pub conditions: Vec<Json>,
    /// The request's `from` join node as Exasol sent it.
    pub tree: Json,
}

impl DetectedJoin {
    pub(crate) fn has_outer(&self) -> bool {
        self.tables.iter().any(|leaf| leaf.null_supplying)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JoinShape {
    NotAJoin,
    Join(DetectedJoin),
}

/// Leaves in left-to-right order, conditions in post-order. A leaf without `alias` is an
/// alias-less occurrence, a leg identity of its own. A leaf under the nullable side of any
/// outer join node is null-supplying: its rows may be replaced by NULLs in the output.
fn collect_join_tree(
    node: &Json,
    nullable: bool,
    leaves: &mut Vec<(String, Option<String>, bool)>,
    conditions: &mut Vec<Json>,
) {
    if node["type"] == "join" {
        let join_type = node["join_type"].as_str().unwrap_or("inner");
        let left_nullable = nullable || matches!(join_type, "right_outer" | "full_outer");
        let right_nullable = nullable || matches!(join_type, "left_outer" | "full_outer");
        collect_join_tree(&node["left"], left_nullable, leaves, conditions);
        collect_join_tree(&node["right"], right_nullable, leaves, conditions);
        conditions.push(node["condition"].clone());
        return;
    }
    let name = node["name"]
        .as_str()
        .expect("Exasol names the table of every join leaf");
    let alias = node["alias"].as_str().map(str::to_string);
    leaves.push((name.to_string(), alias, nullable));
}

/// No equi-condition gate here: only broadcast eligibility (in [`plan_join`]) requires one;
/// the N-scan fallback renders any inner condition. A leaf without a readable identifier note is
/// the same stale-schema condition the single-table path reports, so a hard `Err`.
pub(crate) fn detect_join(request: &Json, pushdown_req: &Json) -> Result<JoinShape, UdfError> {
    let from = match pushdown_req.get("from") {
        Some(from) => from,
        None => return Ok(JoinShape::NotAJoin),
    };
    if from.get("type").and_then(|t| t.as_str()) != Some("join") {
        return Ok(JoinShape::NotAJoin);
    }

    let mut leaves = Vec::new();
    let mut conditions = Vec::new();
    collect_join_tree(from, false, &mut leaves, &mut conditions);

    let mut tables = Vec::with_capacity(leaves.len());
    for (table_name, table_alias, null_supplying) in leaves {
        let table_identifier = read_identifier(request, &table_name)?;
        tables.push(JoinLeaf {
            table_name,
            table_alias,
            table_identifier,
            null_supplying,
        });
    }

    Ok(JoinShape::Join(DetectedJoin {
        tables,
        conditions,
        tree: from.clone(),
    }))
}

/// Resolved once per query in the VS layer, never per shard. `total_bytes` sums the
/// resolved file sizes (manifest, Delta `add`, or listing; no Parquet read) and is what the
/// broadcast threshold is evaluated against.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedJoinSide {
    pub table_name: String,
    pub table_identifier: String,
    /// Per side, never merged: files carry their own delete mechanisms, and a refused column
    /// belongs to the table that raised it.
    pub scan: ResolvedScan,
    pub total_bytes: u64,
    /// Unqualified.
    pub column_types: ColumnTypes,
}

impl ResolvedJoinSide {
    /// Saturating sum: an overflowing total clamps to `u64::MAX`, i.e. over any threshold.
    pub(super) fn new(
        table_name: String,
        table_identifier: String,
        scan: ResolvedScan,
        column_types: ColumnTypes,
    ) -> Self {
        let total_bytes = scan
            .files
            .iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.size));
        Self {
            table_name,
            table_identifier,
            scan,
            total_bytes,
            column_types,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinSides {
    /// Larger side by bytes, sharded like the single-table scan.
    pub fact: ResolvedJoinSide,
    /// Smaller side by bytes, the broadcast candidate.
    pub dimension: ResolvedJoinSide,
    /// The dimension's leg in FROM-tree order, which names its relation alias.
    pub dimension_leg: usize,
    pub join_type: JoinType,
}

impl JoinSides {
    /// Broadcast is two-leg only, so the fact is the leg the dimension is not.
    pub(crate) fn fact_leg(&self) -> usize {
        1 - self.dimension_leg
    }
}

/// `a` is leg 0 and `b` leg 1. On an exact byte tie `a`, the first leg in FROM-tree order,
/// becomes the dimension, so the choice of the sharded occurrence is deterministic.
pub(super) fn select_broadcast_sides(a: ResolvedJoinSide, b: ResolvedJoinSide) -> JoinSides {
    let (dimension, fact, dimension_leg) = if a.total_bytes <= b.total_bytes {
        (a, b, 0)
    } else {
        (b, a, 1)
    };
    JoinSides {
        fact,
        dimension,
        dimension_leg,
        join_type: JoinType::Inner,
    }
}

/// A `left_outer` join broadcasts only its null-supplying right leg: each shard then sees every
/// dimension row a fact row could match, so it alone decides that a fact row has no match.
pub(super) fn select_left_outer_broadcast_sides(
    preserved: ResolvedJoinSide,
    nullable: ResolvedJoinSide,
) -> JoinSides {
    JoinSides {
        fact: preserved,
        dimension: nullable,
        dimension_leg: 1,
        join_type: JoinType::LeftOuter,
    }
}

/// All sides share one per-request `resolver`, so a join costs no more catalog auth
/// round-trips than a single scan. `filter_json` is the leg-local sub-predicate
/// ([`super::rendering::leg_local_filter`]); pruning by it is sound for an inner join.
pub(super) async fn resolve_one_join_side(
    table_name: &str,
    table_identifier: &str,
    resolver: &TableScanResolver<'_>,
    filter_json: Option<&Json>,
    involved: &InvolvedColumns<'_>,
) -> Result<ResolvedJoinSide, UdfError> {
    let (resolved, column_types) = resolver
        .resolve(table_identifier, filter_json, involved)
        .await?;
    Ok(ResolvedJoinSide::new(
        table_name.to_string(),
        table_identifier.to_string(),
        resolved,
        column_types,
    ))
}

/// One leg's column types and notes from the same pass.
pub(in super::super) fn involved_table<'r>(
    request: &'r Json,
    table_name: &str,
) -> Result<InvolvedColumns<'r>, UdfError> {
    involved_columns(request, |tables| {
        tables
            .iter()
            .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(table_name))
    })
}

/// The window of a request that does not aggregate. [`Self::BroadcastUnservable`] falls through
/// to the fallback, never an error.
#[derive(Debug, Clone, PartialEq)]
pub(in super::super) enum JoinWindowPlan {
    Unbounded,
    /// The cap composes: each shard truncates at `n` and the merge truncates again.
    BareLimit(u64),
    /// [`place_broadcast_window`] decides whether this lands on the join block, an outer
    /// wrapper, or both.
    Ordered {
        keys: Vec<ParsedSortKey>,
        limit: Option<u64>,
        offset: u64,
    },
    BroadcastUnservable,
}

/// Whether the projection can bind an `Ordered` key is decided at construction, once the broadcast
/// render has built that projection. An aggregating request never reaches here: Exasol renders
/// its window over the fan-out.
pub(super) fn classify_join_window(pushdown_req: &Json) -> JoinWindowPlan {
    let limit = extract_limit(pushdown_req);
    let offset = extract_offset(pushdown_req);
    let Some(order_by) = pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|elements| !elements.is_empty())
    else {
        // Exasol rejects OFFSET without ORDER BY and a per-shard offset does not compose.
        if offset != 0 {
            return JoinWindowPlan::BroadcastUnservable;
        }
        return match limit {
            Some(n) => JoinWindowPlan::BareLimit(n),
            None => JoinWindowPlan::Unbounded,
        };
    };

    let mut keys = Vec::with_capacity(order_by.len());
    for element in order_by {
        // The wrapper binds ORDER BY against emitted columns and appends no hidden ones, so
        // only a flagged plain column, never an expression, is servable.
        let Some(key) = parse_sort_key_element(element) else {
            return JoinWindowPlan::BroadcastUnservable;
        };
        keys.push(ParsedSortKey::Column(key));
    }
    JoinWindowPlan::Ordered {
        keys,
        limit,
        offset,
    }
}

#[cfg(test)]
#[path = "planning_tests.rs"]
mod tests;
