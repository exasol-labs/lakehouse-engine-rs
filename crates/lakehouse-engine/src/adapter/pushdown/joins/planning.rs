use crate::scan::spec::{FileEntry, LogicalField, NameMappingEntry, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::super::scan_resolution::TableScanResolver;
use super::super::support::{column_types, extract_limit, extract_offset};
use super::super::topn::{ParsedSortKey, parse_sort_key_element};
use super::super::{RefusedColumn, ResolvedScan};

/// Every inner join of any arity is served, so an ineligible shape is the last resort,
/// routed to a hard client-facing error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IneligibleJoinReason {
    /// A join node anywhere in the tree is not inner; a cross join plus WHERE cannot reproduce
    /// outer-join semantics.
    NotInnerJoinType,
    UnsupportedShape,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinLeaf {
    pub table_name: String,
    /// Verbatim (Exasol does not fold it). `None` when absent: Exasol omits the key, and an
    /// alias-less occurrence is a distinct leg identity.
    pub table_alias: Option<String>,
    /// Original-cased catalog identifier from `TABLE_MAP`.
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
    Ineligible(IneligibleJoinReason),
    Join(DetectedJoin),
}

/// Leaves in left-to-right order, conditions in post-order. A leaf without `alias` is an
/// alias-less occurrence, a leg identity of its own.
fn collect_join_tree(
    node: &Json,
    leaves: &mut Vec<(String, Option<String>)>,
    conditions: &mut Vec<Json>,
) -> Result<(), IneligibleJoinReason> {
    match node.get("type").and_then(|t| t.as_str()) {
        Some("join") => {
            let is_inner = node
                .get("join_type")
                .and_then(|t| t.as_str())
                .is_some_and(|t| t.eq_ignore_ascii_case("inner"));
            if !is_inner {
                return Err(IneligibleJoinReason::NotInnerJoinType);
            }
            let (left, right) = match (node.get("left"), node.get("right")) {
                (Some(left), Some(right)) => (left, right),
                _ => return Err(IneligibleJoinReason::UnsupportedShape),
            };
            let condition = match node.get("condition").filter(|c| !c.is_null()) {
                Some(condition) => condition.clone(),
                None => return Err(IneligibleJoinReason::UnsupportedShape),
            };
            collect_join_tree(left, leaves, conditions)?;
            collect_join_tree(right, leaves, conditions)?;
            conditions.push(condition);
            Ok(())
        }
        Some("table") => match node.get("name").and_then(|n| n.as_str()) {
            Some(name) => {
                let alias = node
                    .get("alias")
                    .and_then(|a| a.as_str())
                    .map(str::to_string);
                leaves.push((name.to_string(), alias));
                Ok(())
            }
            None => Err(IneligibleJoinReason::UnsupportedShape),
        },
        _ => Err(IneligibleJoinReason::UnsupportedShape),
    }
}

/// No equi-condition gate here: only broadcast eligibility (in [`plan_join`]) requires one;
/// the N-scan fallback renders any inner condition. A leaf absent from `TABLE_MAP` is the
/// same stale-schema condition the single-table path reports, so a hard `Err`.
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
    if let Err(reason) = collect_join_tree(from, &mut leaves, &mut conditions) {
        return Ok(JoinShape::Ineligible(reason));
    }

    let table_map = crate::adapter::read_table_map(request);
    let mut tables = Vec::with_capacity(leaves.len());
    for (table_name, table_alias) in leaves {
        let table_identifier = table_map.get(&table_name).cloned().ok_or_else(|| {
            UdfError::User(format!(
                "pushdown: virtual table '{table_name}' is not in TABLE_MAP; \
                 drop and recreate the virtual schema"
            ))
        })?;
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
    /// Includes positional deletes, so the scan applies them per side.
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
}

impl ResolvedJoinSide {
    /// Saturating sum: an overflowing total clamps to `u64::MAX`, i.e. over any threshold.
    pub(super) fn new(
        table_name: String,
        table_identifier: String,
        resolved: ResolvedScan,
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
        }
    }
}

/// Both sides stay fully resolved because the N-scan fallback scans both;
/// `broadcast_eligible` only routes between the two builders and is never an error.
///
/// A self-join ties, so the left side becomes the dimension; broadcasting a table against
/// itself is a correct inner join, and the disjoint-column-name guard declines it anyway.
/// An empty side has 0 bytes and is always the dimension; selection does not special-case
/// it so role assignment stays total.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinSides {
    /// Larger side by bytes, sharded like the single-table scan.
    pub fact: ResolvedJoinSide,
    /// Smaller side by bytes, the broadcast candidate.
    pub dimension: ResolvedJoinSide,
    pub broadcast_eligible: bool,
}

/// On an exact byte tie `a` becomes the dimension (arbitrary but deterministic).
pub(super) fn select_broadcast_sides(
    a: ResolvedJoinSide,
    b: ResolvedJoinSide,
    join_broadcast_max_bytes: u64,
) -> JoinSides {
    let (dimension, fact) = if a.total_bytes <= b.total_bytes {
        (a, b)
    } else {
        (b, a)
    };
    let broadcast_eligible = dimension.total_bytes <= join_broadcast_max_bytes;
    JoinSides {
        fact,
        dimension,
        broadcast_eligible,
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
    declared_columns: &[(String, String)],
) -> Result<ResolvedJoinSide, UdfError> {
    let resolved = resolver
        .resolve(table_identifier, filter_json, declared_columns)
        .await?;
    Ok(ResolvedJoinSide::new(
        table_name.to_string(),
        table_identifier.to_string(),
        resolved,
    ))
}

/// CROSS-FOLD SEAM: the result is string-matched in `referenced_leg_columns` against names
/// folded with ASCII-only `to_ascii_uppercase`, while `column_types` folds differently. Do
/// not reconcile the folds (see `walk_column_nodes` and
/// `vs-adapter/pushdown-module-structure`). They agree by premise:
/// `build_listing_virtual_tables` Unicode-uppercases every declared name, leaving no ASCII
/// lowercase for either fold to touch. E2E `non_ascii_table_and_column_stay_queryable`
/// guards that premise.
pub(super) fn involved_table_columns(request: &Json, table_name: &str) -> Vec<(String, String)> {
    column_types(request, |tables: &[Json]| {
        tables
            .iter()
            .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(table_name))
    })
}

/// `true` when no column name appears on both sides. The translator renders bare column
/// references, which resolve unambiguously against the combined schema only then; a
/// collision declines cleanly to the fallback. Both inputs are already uppercased.
pub(super) fn disjoint_schema_guard(left: &[(String, String)], right: &[(String, String)]) -> bool {
    let left_names: std::collections::HashSet<&str> =
        left.iter().map(|(n, _)| n.as_str()).collect();
    !right.iter().any(|(n, _)| left_names.contains(n.as_str()))
}

/// The broadcast in-UDF join renders only projection, filter, and join condition, so any
/// aggregation must be executed by Exasol over the materialized join.
fn carries_aggregation_clause(pushdown_req: &Json) -> bool {
    let has_aggregate_item = pushdown_req
        .get("selectList")
        .and_then(|v| v.as_array())
        .is_some_and(|list| {
            list.iter()
                .any(|item| item.get("type").and_then(|t| t.as_str()) == Some("function_aggregate"))
        });
    let has_group_by = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty());
    let is_group_by_aggregation =
        pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by");
    let has_having = pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .is_some();
    has_aggregate_item || has_group_by || is_group_by_aggregation || has_having
}

/// [`Self::ExasolPostProcessed`] falls through to the fallback, never an error.
#[derive(Debug)]
pub(in super::super) enum JoinWindowPlan {
    Unbounded,
    /// The cap composes: each shard truncates at `n` and the merge truncates again.
    BareLimit(u64),
    /// Served by an outer wrapper over the merged fan-out; a per-shard `OFFSET` would skip
    /// each shard's own first rows.
    Ordered {
        keys: Vec<ParsedSortKey>,
        limit: Option<u64>,
        offset: u64,
    },
    ExasolPostProcessed,
}

/// Whether the projection can bind an `Ordered` key is decided at construction: rendering
/// a projection here would let an aggregate-carrying join reach a render that can `Err` on
/// absent column metadata.
pub(super) fn classify_join_window(pushdown_req: &Json) -> JoinWindowPlan {
    if carries_aggregation_clause(pushdown_req) {
        return JoinWindowPlan::ExasolPostProcessed;
    }
    let limit = extract_limit(pushdown_req);
    let offset = extract_offset(pushdown_req);
    let Some(order_by) = pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|elements| !elements.is_empty())
    else {
        // Exasol rejects OFFSET without ORDER BY and a per-shard offset does not compose.
        if offset != 0 {
            return JoinWindowPlan::ExasolPostProcessed;
        }
        return match limit {
            Some(n) => JoinWindowPlan::BareLimit(n),
            None => JoinWindowPlan::Unbounded,
        };
    };

    let mut keys = Vec::with_capacity(order_by.len());
    for element in order_by {
        // The wrapper binds ORDER BY against emitted columns and appends no hidden ones, so
        // only a flagged bare column is servable.
        let Some(key) = parse_sort_key_element(element) else {
            return JoinWindowPlan::ExasolPostProcessed;
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
