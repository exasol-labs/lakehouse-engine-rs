use crate::adapter::table_identity::read_identifier;
use crate::scan::spec::{FileEntry, LogicalField, NameMappingEntry, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::super::scan_resolution::TableScanResolver;
use super::super::support::{
    ColumnTypes, InvolvedColumns, extract_limit, extract_offset, involved_columns,
};
use super::super::topn::{ParsedSortKey, parse_sort_key_element};
use super::super::{RefusedColumn, ResolvedScan};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinLeaf {
    pub table_name: String,
    /// Verbatim (Exasol does not fold it). `None` when absent: Exasol omits the key, and an
    /// alias-less occurrence is a distinct leg identity.
    pub table_alias: Option<String>,
    /// Original-cased catalog identifier from the identifier note of the leg's involved table.
    pub table_identifier: String,
}

/// `conditions` are AND-conjoined by the N-scan fallback, which is order-agnostic for an
/// all-inner join.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DetectedJoin {
    /// Stable left-to-right tree order.
    pub tables: Vec<JoinLeaf>,
    pub conditions: Vec<Json>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JoinShape {
    NotAJoin,
    Join(DetectedJoin),
}

/// Leaves in left-to-right order, conditions in post-order. A leaf without `alias` is an
/// alias-less occurrence, a leg identity of its own. Exasol pushes only the inner equi-joins
/// the adapter advertises, so every join node is inner with both sides and a condition.
fn collect_join_tree(
    node: &Json,
    leaves: &mut Vec<(String, Option<String>)>,
    conditions: &mut Vec<Json>,
) {
    if node["type"] == "join" {
        collect_join_tree(&node["left"], leaves, conditions);
        collect_join_tree(&node["right"], leaves, conditions);
        conditions.push(node["condition"].clone());
        return;
    }
    let name = node["name"]
        .as_str()
        .expect("Exasol names the table of every join leaf");
    let alias = node["alias"].as_str().map(str::to_string);
    leaves.push((name.to_string(), alias));
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
    collect_join_tree(from, &mut leaves, &mut conditions);

    let mut tables = Vec::with_capacity(leaves.len());
    for (table_name, table_alias) in leaves {
        let table_identifier = read_identifier(request, &table_name)?;
        tables.push(JoinLeaf {
            table_name,
            table_alias,
            table_identifier,
        });
    }

    Ok(JoinShape::Join(DetectedJoin { tables, conditions }))
}

/// Resolved once per query in the VS layer, never per shard. `total_bytes` sums the
/// resolved file sizes (manifest, Delta `add`, or listing; no Parquet read) and is what the
/// broadcast threshold is evaluated against.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedJoinSide {
    pub table_name: String,
    pub table_identifier: String,
    /// Empty ⇒ every `files` path is absolute.
    pub table_root: String,
    /// Includes each file's delete mechanisms, so the scan applies them per side.
    pub files: Vec<FileEntry>,
    pub logical_schema: Vec<LogicalField>,
    /// Empty when the table has no name-mapping property, and on every Delta side.
    pub name_mapping: Vec<NameMappingEntry>,
    pub effective_storage: StorageBackend,
    /// Empty on every Iceberg side.
    pub partition_columns: Vec<String>,
    pub total_bytes: u64,
    /// Per side, not merged: a refusal belongs to the table that raised it.
    pub refused_columns: Vec<RefusedColumn>,
    /// Unqualified.
    pub column_types: ColumnTypes,
}

impl ResolvedJoinSide {
    /// Saturating sum: an overflowing total clamps to `u64::MAX`, i.e. over any threshold.
    pub(super) fn new(
        table_name: String,
        table_identifier: String,
        resolved: ResolvedScan,
        column_types: ColumnTypes,
    ) -> Self {
        let ResolvedScan {
            files,
            effective_storage,
            logical_schema,
            table_root,
            name_mapping,
            partition_columns,
            refused_columns,
        } = resolved;
        let total_bytes = files
            .iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.size));
        Self {
            table_name,
            table_identifier,
            table_root,
            files,
            logical_schema,
            name_mapping,
            effective_storage,
            partition_columns,
            total_bytes,
            refused_columns,
            column_types,
        }
    }
}

/// Both sides stay fully resolved because the N-scan fallback scans both;
/// `broadcast_eligible` only routes between the two builders and is never an error.
///
/// Broadcasting a table against itself is a correct inner join: each occurrence is its own leg
/// with its own relation alias. Each side's bytes are taken after its own leg-local pruning, so
/// a self-join's occurrences can differ in size. An empty side has 0 bytes and is always the
/// dimension; selection does not special-case it so role assignment stays total.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinSides {
    /// Larger side by bytes, sharded like the single-table scan.
    pub fact: ResolvedJoinSide,
    /// Smaller side by bytes, the broadcast candidate.
    pub dimension: ResolvedJoinSide,
    pub broadcast_eligible: bool,
    /// The dimension's leg in FROM-tree order, which names its relation alias.
    pub dimension_leg: usize,
}

impl JoinSides {
    /// Broadcast is two-leg only, so the fact is the leg the dimension is not.
    pub(crate) fn fact_leg(&self) -> usize {
        1 - self.dimension_leg
    }
}

/// `a` is leg 0 and `b` leg 1. On an exact byte tie `a`, the first leg in FROM-tree order,
/// becomes the dimension, so the choice of the sharded occurrence is deterministic.
pub(super) fn select_broadcast_sides(
    a: ResolvedJoinSide,
    b: ResolvedJoinSide,
    join_broadcast_max_bytes: u64,
) -> JoinSides {
    let (dimension, fact, dimension_leg) = if a.total_bytes <= b.total_bytes {
        (a, b, 0)
    } else {
        (b, a, 1)
    };
    let broadcast_eligible = dimension.total_bytes <= join_broadcast_max_bytes;
    JoinSides {
        fact,
        dimension,
        broadcast_eligible,
        dimension_leg,
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
